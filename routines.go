package jack

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// RoutineState describes the lifecycle state of a tracked goroutine.
type RoutineState int32

const (
	RoutineRunning RoutineState = iota
	RoutineDone
	RoutinePanicked
	RoutineCancelled
)

// String returns a human-readable label for the goroutine state.
func (s RoutineState) String() string {
	switch s {
	case RoutineRunning:
		return "running"
	case RoutineDone:
		return "done"
	case RoutinePanicked:
		return "panicked"
	case RoutineCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// RoutineInfo holds the observable state of a single tracked goroutine.
type RoutineInfo struct {
	ID        string
	Label     string
	State     RoutineState
	StartedAt time.Time
	EndedAt   time.Time
	Err       error
	Stack     []byte // non-nil only when State == RoutinePanicked
}

// RoutinesMetrics tracks aggregate goroutine statistics.
type RoutinesMetrics struct {
	Spawned   atomic.Uint64 // total goroutines ever started
	Active    atomic.Int64  // currently running
	Completed atomic.Uint64 // finished without error or panic
	Failed    atomic.Uint64 // returned a non-nil error
	Panicked  atomic.Uint64 // recovered from a panic
	Cancelled atomic.Uint64 // stopped via context cancellation
}

// RoutineOption configures a Routines tracker.
type RoutineOption func(*Routines)

// RoutineWithOnPanic sets a callback invoked whenever a goroutine panics.
// The callback runs synchronously before the goroutine exits.
func RoutineWithOnPanic(fn func(info RoutineInfo)) RoutineOption {
	return func(r *Routines) { r.onPanic = fn }
}

// RoutineWithOnDone sets a callback invoked when any goroutine finishes
// (regardless of success, error, or panic).
func RoutineWithOnDone(fn func(info RoutineInfo)) RoutineOption {
	return func(r *Routines) { r.onDone = fn }
}

// RoutineWithIDGenerator replaces the default sequential ID generator.
func RoutineWithIDGenerator(fn func(label string) string) RoutineOption {
	return func(r *Routines) { r.idGen = fn }
}

// RoutineWithMaxEntries caps the number of retained RoutineInfo entries.
// When exceeded, completed entries are evicted to bound memory growth.
func RoutineWithMaxEntries(n int) RoutineOption {
	return func(r *Routines) { r.maxEntries = n }
}

// Routines is a goroutine tracker and lifecycle manager.
// It is the answer to the "rogue goroutine" problem: every goroutine spawned
// through Routines is registered, tracked by state, and guaranteed to be
// joined by Stop or Wait.
//
// Singleton usage:
//
//	var rt = jack.NewRoutines()
//
//	rt.Spawn("fetch", func(ctx context.Context) error { ... })
//	rt.SpawnCtx("heartbeat", func(ctx context.Context) error { ... })
//
//	rt.Stop()   // cancels context, waits for all goroutines
//
// All methods are safe for concurrent use.
type Routines struct {
	mu      sync.RWMutex
	entries map[string]*RoutineInfo
	cancels map[string]context.CancelFunc

	ctx    context.Context
	cancel context.CancelFunc

	wg      sync.WaitGroup
	counter atomic.Uint64

	onPanic func(info RoutineInfo)
	onDone  func(info RoutineInfo)
	idGen   func(label string) string

	metrics    *RoutinesMetrics
	maxEntries int
	closed     atomic.Bool
}

// NewRoutines creates a Routines tracker. The returned tracker has its own
// cancellable context; call Stop() to terminate all goroutines it owns.
func NewRoutines(opts ...RoutineOption) *Routines {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Routines{
		entries: make(map[string]*RoutineInfo),
		cancels: make(map[string]context.CancelFunc),
		ctx:     ctx,
		cancel:  cancel,
		metrics: &RoutinesMetrics{},
		idGen: func(label string) string {
			return label // sequential ID appended in Go()
		},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Metrics returns aggregate goroutine statistics.
func (r *Routines) Metrics() *RoutinesMetrics { return r.metrics }

// Spawn spawns a goroutine under the tracker's context.
// The goroutine is cancelled when Stop() is called or the tracker's context
// is cancelled. label is used for identification; it need not be unique —
// a unique ID is derived automatically.
// Returns the assigned ID so the caller can query status via Info(id).
func (r *Routines) Spawn(label string, fn func(context.Context) error) string {
	ctx, cancel := context.WithCancel(r.ctx)
	return r.spawnWithCancel(label, fn, ctx, cancel)
}

// SpawnCtx spawns a goroutine under a caller-supplied context. The goroutine
// is cancelled when either the supplied ctx OR the tracker's context is done —
// whichever fires first.
func (r *Routines) SpawnCtx(ctx context.Context, label string, fn func(context.Context) error) string {
	merged, cancel := mergeContexts(r.ctx, ctx)
	return r.spawnWithCancel(label, fn, merged, cancel)
}

// Background spawns a long-running goroutine that is restarted automatically
// if it returns a non-nil error, up to maxRestarts times (0 = unlimited).
// Each restart is counted in Metrics.Spawned.
func (r *Routines) Background(label string, maxRestarts int, fn func(context.Context) error) string {
	id := r.nextID(label)
	r.wg.Add(1)
	r.metrics.Spawned.Add(1)
	r.register(id, label)
	ctx, cancel := context.WithCancel(r.ctx)
	r.mu.Lock()
	r.cancels[id] = cancel
	r.mu.Unlock()
	go func() {
		defer r.wg.Done()
		defer cancel()
		defer func() {
			r.mu.Lock()
			delete(r.cancels, id)
			r.mu.Unlock()
		}()
		restarts := 0
		ran := false
		for {
			if r.closed.Load() || r.ctx.Err() != nil {
				if !ran {
					r.finish(id, RoutineCancelled, nil, nil)
					r.metrics.Cancelled.Add(1)
				}
				return
			}
			r.metrics.Active.Add(1)
			r.resetState(id)
			err := r.runOne(id, fn, ctx)
			r.metrics.Active.Add(-1)
			ran = true
			if err == nil || ctx.Err() != nil {
				return
			}
			if maxRestarts > 0 {
				restarts++
				if restarts >= maxRestarts {
					return
				}
			}
			r.metrics.Spawned.Add(1)
		}
	}()
	return id
}

// Cancel cancels a single goroutine by ID. Returns false if the ID is unknown
// or the goroutine has already exited.
func (r *Routines) Cancel(id string) bool {
	r.mu.Lock()
	cancel, ok := r.cancels[id]
	r.mu.Unlock()
	if !ok {
		return false
	}
	cancel()
	return true
}

// Stop cancels the tracker's context and waits for all goroutines to exit.
// After Stop returns, no goroutines tracked by this Routines are running.
func (r *Routines) Stop() {
	if r.closed.CompareAndSwap(false, true) {
		r.cancel()
	}
	r.wg.Wait()
}

// StopTimeout cancels the tracker's context and waits up to the given duration
// for all goroutines to exit. Returns an error if the timeout is exceeded.
func (r *Routines) StopTimeout(timeout time.Duration) error {
	if r.closed.CompareAndSwap(false, true) {
		r.cancel()
	}
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New("routines stop timeout exceeded")
	}
}

// Wait blocks until all currently tracked goroutines have exited.
// Unlike Stop, it does not cancel the context.
func (r *Routines) Wait() {
	r.wg.Wait()
}

// Info returns a snapshot of the named goroutine's current state.
// Returns false if no goroutine with that ID exists.
func (r *Routines) Info(id string) (RoutineInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.entries[id]
	if !ok {
		return RoutineInfo{}, false
	}
	return *info, true
}

// List returns a snapshot of all tracked goroutines, including completed ones.
func (r *Routines) List() []RoutineInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RoutineInfo, 0, len(r.entries))
	for _, v := range r.entries {
		out = append(out, *v)
	}
	return out
}

// Forget removes a completed goroutine from tracking, reclaiming the memory
// held by its RoutineInfo. Returns false if the ID is unknown or the
// goroutine is still running.
func (r *Routines) Forget(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.entries[id]
	if !ok || info.State == RoutineRunning {
		return false
	}
	delete(r.entries, id)
	return true
}

// Active returns how many goroutines are currently in RoutineRunning state.
func (r *Routines) Active() int { return int(r.metrics.Active.Load()) }

// internal

func (r *Routines) spawnWithCancel(label string, fn func(context.Context) error, ctx context.Context, cancel context.CancelFunc) string {
	if r.closed.Load() {
		cancel()
		return ""
	}
	id := r.nextID(label)
	r.wg.Add(1)
	r.metrics.Spawned.Add(1)
	r.metrics.Active.Add(1)
	r.register(id, label)
	r.mu.Lock()
	r.cancels[id] = cancel
	r.mu.Unlock()
	go func() {
		defer r.wg.Done()
		defer r.metrics.Active.Add(-1)
		defer cancel()
		defer func() {
			r.mu.Lock()
			delete(r.cancels, id)
			r.mu.Unlock()
		}()
		r.runOne(id, fn, ctx)
	}()
	return id
}

// runOne executes fn with panic recovery and updates the goroutine's state on exit.
func (r *Routines) runOne(id string, fn func(context.Context) error, ctx context.Context) (err error) {
	defer func() {
		if p := recover(); p != nil {
			stack := debug.Stack()
			panicErr := fmt.Errorf("panic: %v", p)
			r.finish(id, RoutinePanicked, panicErr, stack)
			r.metrics.Panicked.Add(1)
			if r.onPanic != nil {
				info, _ := r.Info(id)
				r.onPanic(info)
			}
			err = panicErr
		}
	}()

	err = fn(ctx)
	if err != nil {
		if ctx.Err() != nil {
			r.finish(id, RoutineCancelled, err, nil)
			r.metrics.Cancelled.Add(1)
		} else {
			r.finish(id, RoutineDone, err, nil)
			r.metrics.Failed.Add(1)
		}
	} else {
		r.finish(id, RoutineDone, nil, nil)
		r.metrics.Completed.Add(1)
	}
	return err
}

// register creates the RoutineInfo entry before the goroutine starts.
func (r *Routines) register(id, label string) {
	info := &RoutineInfo{
		ID:        id,
		Label:     label,
		State:     RoutineRunning,
		StartedAt: time.Now(),
	}
	r.mu.Lock()
	r.entries[id] = info
	r.mu.Unlock()
}

// resetState sets a goroutine back to Running for Background restarts.
func (r *Routines) resetState(id string) {
	r.mu.Lock()
	if info, ok := r.entries[id]; ok {
		info.State = RoutineRunning
		info.EndedAt = time.Time{}
	}
	r.mu.Unlock()
}

// finish records the terminal state and invokes the onDone callback.
func (r *Routines) finish(id string, state RoutineState, err error, stack []byte) {
	r.mu.Lock()
	info, ok := r.entries[id]
	if ok {
		info.State = state
		info.Err = err
		info.EndedAt = time.Now()
		info.Stack = stack
	}
	r.evictIfNeeded()
	r.mu.Unlock()
	if ok && r.onDone != nil {
		r.onDone(*info)
	}
}

// evictIfNeeded drops completed entries when maxEntries is exceeded.
func (r *Routines) evictIfNeeded() {
	if r.maxEntries <= 0 {
		return
	}
	for len(r.entries) > r.maxEntries {
		var victim string
		for id, info := range r.entries {
			if info.State != RoutineRunning {
				victim = id
				break
			}
		}
		if victim == "" {
			break
		}
		delete(r.entries, victim)
	}
}

// nextID returns a unique label-scoped ID using a monotonic counter.
func (r *Routines) nextID(label string) string {
	n := r.counter.Add(1)
	return fmt.Sprintf("%s#%d", label, n)
}

// mergeContexts returns a context that is cancelled when either a or b is done.
// Uses context.AfterFunc to avoid leaking a goroutine when b is never cancelled.
func mergeContexts(a, b context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(a)
	stop := context.AfterFunc(b, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// defaultRoutines is a package-level singleton for applications that want a
// single global goroutine tracker without passing it around explicitly.
var defaultRoutines = NewRoutines()

func Spawn(label string, fn func(context.Context) error) {
	defaultRoutines.Spawn(label, fn)
}

func Background(label string, maxRestarts int, fn func(context.Context) error) string {
	return defaultRoutines.Background(label, maxRestarts, fn)
}

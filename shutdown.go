package jack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/ll"
)

// ShutdownOption configures the Shutdown.
type ShutdownOption func(*Shutdown)

// ShutdownWithTimeout sets the maximum time to wait for shutdown completion.
func ShutdownWithTimeout(d time.Duration) ShutdownOption {
	return func(sm *Shutdown) { sm.timeout = d }
}

// ShutdownConcurrent enables concurrent execution of cleanup functions.
// By default, execution is sequential (LIFO).
func ShutdownConcurrent() ShutdownOption {
	return func(sm *Shutdown) { sm.concurrent = true }
}

// ShutdownWithSignals specifies which OS signals to capture.
// If not set, defaults to SIGINT, SIGTERM, and SIGQUIT.
func ShutdownWithSignals(signals ...os.Signal) ShutdownOption {
	return func(sm *Shutdown) { sm.signals = signals }
}

// ShutdownWithForceQuit enables a force-quit trigger after a specific timeout,
// cancelling the cleanup context if shutdown takes too long.
func ShutdownWithForceQuit(d time.Duration) ShutdownOption {
	return func(sm *Shutdown) { sm.forceQuitTimeout = d }
}

// ShutdownWithLogger sets a custom logger for the manager.
func ShutdownWithLogger(l *ll.Logger) ShutdownOption {
	return func(sm *Shutdown) {
		if l != nil {
			sm.logger = l.Namespace("shutdown")
		}
	}
}

// Shutdown manages the graceful shutdown process.
type Shutdown struct {
	mu              sync.RWMutex
	signalChan      chan os.Signal
	doneChan        chan struct{}
	forceQuit       chan struct{}
	events          []namedCall
	priorityEvents  map[int][]namedCall
	usePrioritySort bool
	inShutdown      atomic.Bool
	shutdownCtx     context.Context
	cancelFunc      context.CancelFunc

	timeout          time.Duration
	concurrent       bool
	signals          []os.Signal
	forceQuitTimeout time.Duration
	logger           *ll.Logger

	statsMu sync.RWMutex
	stats   *ShutdownStats
}

type namedCall struct {
	Name     string
	Fn       FuncCtx
	Priority int
}

type ShutdownStats struct {
	TotalEvents     int
	CompletedEvents int
	FailedEvents    int
	StartTime       time.Time
	EndTime         time.Time
	Errors          []error
}

// NewShutdown creates a configured Shutdown manager.
// Defaults: 30s timeout, sequential execution, SIGINT/SIGTERM/SIGQUIT.
func NewShutdown(opts ...ShutdownOption) *Shutdown {
	sm := &Shutdown{
		timeout:    30 * time.Second,
		concurrent: false,
		signals: []os.Signal{
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT,
		},
		doneChan:       make(chan struct{}),
		priorityEvents: make(map[int][]namedCall),
		stats:          &ShutdownStats{Errors: make([]error, 0)},
	}
	if logger != nil {
		sm.logger = logger.Namespace("shutdown")
	} else {
		sm.logger = &ll.Logger{}
	}
	for _, opt := range opts {
		opt(sm)
	}
	sm.signalChan = make(chan os.Signal, 1)
	signal.Notify(sm.signalChan, sm.signals...)
	ctx, cancel := context.WithCancel(context.Background())
	sm.shutdownCtx = ctx
	sm.cancelFunc = cancel
	if sm.forceQuitTimeout > 0 {
		sm.forceQuit = make(chan struct{}, 1)
		go sm.forceQuitMonitor(sm.forceQuitTimeout)
	}
	return sm
}

// forceQuitMonitor runs in background and forces context cancellation
// if the configured force-quit timeout is reached before normal completion.
// This ensures the process doesn't hang indefinitely on stuck cleanup tasks.
func (sm *Shutdown) forceQuitMonitor(timeout time.Duration) {
	select {
	case <-sm.shutdownCtx.Done():
		return
	case <-time.After(timeout):
		sm.log("force quit timeout (%v) reached — cancelling context", timeout)
		sm.cancelFunc()
		if sm.forceQuit != nil {
			select {
			case sm.forceQuit <- struct{}{}:
			default:
			}
		}
	}
}

// Register adds a cleanup task. Supported types:
// func(), func() error, func(context.Context) error, io.Closer.
func (sm *Shutdown) Register(fn any) error {
	if fn == nil {
		return errors.New("cannot register nil")
	}
	var name string
	var call FuncCtx
	switch f := fn.(type) {
	case func():
		name = autoName(f)
		call = func(ctx context.Context) error { f(); return nil }
	case func() error:
		name = autoName(f)
		call = func(ctx context.Context) error { return f() }
	case Func:
		name = autoName(f)
		call = func(ctx context.Context) error { return f() }
	case func(context.Context) error:
		name = autoName(f)
		call = f
	case FuncCtx:
		name = autoName(f)
		call = f
	case io.Closer:
		name = fmt.Sprintf("closer:%T", f)
		call = func(ctx context.Context) error { return f.Close() }
	default:
		return fmt.Errorf("unsupported callback type: %T", fn)
	}
	return sm.registerCall(name, call, 0)
}

func (sm *Shutdown) RegisterWithPriority(name string, priority int, fns ...FuncCtx) error {
	if len(fns) == 0 {
		return errors.New("at least one function required")
	}
	if sm.IsShuttingDown() {
		return errors.New("cannot register after shutdown started")
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, fn := range fns {
		if fn == nil {
			return errors.New("callback cannot be nil")
		}
		wrapped := sm.wrapWithPanicRecovery(name, fn)
		sm.priorityEvents[priority] = append(sm.priorityEvents[priority], namedCall{
			Name:     name,
			Fn:       wrapped,
			Priority: priority,
		})
		sm.statsMu.Lock()
		sm.stats.TotalEvents++
		sm.statsMu.Unlock()
	}
	sm.usePrioritySort = true
	return nil
}

func (sm *Shutdown) RegisterFunc(name string, fn func()) error {
	if fn == nil {
		return errors.New("callback cannot be nil")
	}
	return sm.registerCall(name, func(ctx context.Context) error { fn(); return nil }, 0)
}

func (sm *Shutdown) RegisterCall(name string, fn Func) error {
	if fn == nil {
		return errors.New("callback cannot be nil")
	}
	return sm.registerCall(name, func(ctx context.Context) error { return fn() }, 0)
}

// RegisterCloser registers an io.Closer.
// Convenience wrapper that converts Close() error into shutdown error.
// Name reflects the concrete type when left empty.
func (sm *Shutdown) RegisterCloser(name string, closer io.Closer) error {
	if closer == nil {
		return errors.New("closer cannot be nil")
	}
	if name == "" {
		name = fmt.Sprintf("closer:%T", closer)
	}
	return sm.registerCall(name, func(ctx context.Context) error { return closer.Close() }, 0)
}

// RegisterWithContext registers a fully context-aware callback.
// Allows explicit naming and direct use of jack.FuncCtx functions.
// Preferred for advanced cleanup needing cancellation/timeout awareness.
func (sm *Shutdown) RegisterWithContext(name string, fn FuncCtx) error {
	return sm.registerCall(name, fn, 0)
}

func (sm *Shutdown) registerCall(name string, fn FuncCtx, priority int) error {
	if fn == nil {
		return errors.New("callback cannot be nil")
	}
	if name == "" {
		name = runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		if name == "" {
			name = "anonymous"
		}
	}
	if sm.IsShuttingDown() {
		return errors.New("cannot register after shutdown started")
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	wrapped := sm.wrapWithPanicRecovery(name, fn)
	sm.events = append(sm.events, namedCall{Name: name, Fn: wrapped, Priority: priority})
	sm.statsMu.Lock()
	sm.stats.TotalEvents++
	sm.statsMu.Unlock()
	return nil
}

func (sm *Shutdown) wrapWithPanicRecovery(name string, fn FuncCtx) FuncCtx {
	return func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &ShutdownError{
					Name:      name,
					Err:       fmt.Errorf("panic during shutdown: %v", r),
					Timestamp: time.Now(),
				}
				sm.log("PANIC recovered in task '%s': %v", name, r)
			}
		}()
		if callErr := fn(ctx); callErr != nil {
			return &ShutdownError{Name: name, Err: callErr, Timestamp: time.Now()}
		}
		return nil
	}
}

// Wait blocks until a signal is received or TriggerShutdown is called,
// then runs all registered cleanup tasks and returns statistics.
func (sm *Shutdown) Wait() *ShutdownStats {
	if sm.IsShuttingDown() {
		<-sm.doneChan
		return sm.GetStats()
	}
	select {
	case sig := <-sm.signalChan:
		sm.log("received signal: %v", sig)
	case <-sm.forceQuit:
		sm.log("force quit triggered")
	case <-sm.shutdownCtx.Done():
		sm.log("shutdown context cancelled")
	}
	return sm.executeShutdown()
}

// WaitChan returns a channel that receives stats once shutdown is complete.
// Non-blocking alternative to Wait(), ideal for async integration.
// Channel is closed after sending the single stats value.
func (sm *Shutdown) WaitChan() <-chan *ShutdownStats {
	ch := make(chan *ShutdownStats, 1)
	go func() { ch <- sm.Wait(); close(ch) }()
	return ch
}

// TriggerShutdown manually initiates the shutdown process and returns final statistics.
func (sm *Shutdown) TriggerShutdown() *ShutdownStats {
	select {
	case sm.signalChan <- syscall.SIGTERM:
		return sm.Wait()
	default:
		return sm.executeShutdown()
	}
}

// executeShutdown performs the actual cleanup execution.
// Ensures idempotency, clears event list, records timing and errors.
// Called internally by Wait() and TriggerShutdown().
func (sm *Shutdown) executeShutdown() *ShutdownStats {
	if !sm.inShutdown.CompareAndSwap(false, true) {
		<-sm.doneChan
		return sm.GetStats()
	}
	sm.mu.Lock()

	// Snapshot and clear state while holding the lock.
	regularEvents := make([]namedCall, len(sm.events))
	for i, e := range sm.events {
		regularEvents[i] = e
	}
	// Reverse for LIFO.
	for i, j := 0, len(regularEvents)-1; i < j; i, j = i+1, j-1 {
		regularEvents[i], regularEvents[j] = regularEvents[j], regularEvents[i]
	}

	priorityEvents := sm.priorityEvents
	usePriority := sm.usePrioritySort

	sm.events = nil
	sm.priorityEvents = nil
	sm.mu.Unlock()

	// Count total events for stats.
	total := len(regularEvents)
	if usePriority {
		for _, group := range priorityEvents {
			total += len(group)
		}
	}

	sm.statsMu.Lock()
	sm.stats.StartTime = time.Now()
	sm.stats.TotalEvents = total
	sm.statsMu.Unlock()

	sm.log("starting shutdown of %d task(s)", total)

	var cleanupCtx context.Context
	var cleanupCancel context.CancelFunc
	if sm.timeout > 0 {
		cleanupCtx, cleanupCancel = context.WithTimeout(sm.shutdownCtx, sm.timeout)
	} else {
		cleanupCtx, cleanupCancel = context.WithCancel(sm.shutdownCtx)
	}
	defer cleanupCancel()

	if usePriority && len(priorityEvents) > 0 {
		// Execute priority groups as sequential waves — within each wave,
		// tasks run concurrently (or sequentially if not sm.concurrent).
		// This means Listeners (priority 0) fully completes before
		// TrafficManager (priority 1) starts, preserving the ordering
		// guarantee even when ShutdownConcurrent() is configured.
		keys := make([]int, 0, len(priorityEvents))
		for k := range priorityEvents {
			keys = append(keys, k)
		}
		sort.Ints(keys)

		for _, k := range keys {
			group := priorityEvents[k]
			// LIFO within each priority group.
			wave := make([]namedCall, len(group))
			for i, e := range group {
				wave[len(group)-1-i] = e
			}
			sm.log("running priority group %d (%d task(s))", k, len(wave))
			if sm.concurrent {
				sm.executeConcurrent(wave, cleanupCtx)
			} else {
				sm.executeSequential(wave, cleanupCtx)
			}
		}
	}

	// Regular events (RegisterFunc / Register) run last, after all priority
	// groups have completed.
	if len(regularEvents) > 0 {
		if sm.concurrent {
			sm.executeConcurrent(regularEvents, cleanupCtx)
		} else {
			sm.executeSequential(regularEvents, cleanupCtx)
		}
	}

	sm.statsMu.Lock()
	sm.stats.EndTime = time.Now()
	sm.statsMu.Unlock()

	sm.log("shutdown completed in %v (failed: %d)", sm.stats.EndTime.Sub(sm.stats.StartTime), sm.stats.FailedEvents)
	sm.cancelFunc()
	close(sm.doneChan)
	signal.Stop(sm.signalChan)
	return sm.GetStats()
}

// executeSequential runs cleanup tasks in LIFO order (last registered first).
// Blocks until all tasks complete or context is cancelled.
// Updates completion/failure counters for each task.
func (sm *Shutdown) executeSequential(events []namedCall, ctx context.Context) {
	for _, nc := range events {
		sm.log("running: %s (priority: %d)", nc.Name, nc.Priority)
		if err := nc.Fn(ctx); err != nil {
			sm.log("task failed: %s -> %v", nc.Name, err)
			sm.recordError(err)
			sm.statsMu.Lock()
			sm.stats.FailedEvents++
			sm.statsMu.Unlock()
		} else {
			sm.statsMu.Lock()
			sm.stats.CompletedEvents++
			sm.statsMu.Unlock()
		}
	}
}

// executeConcurrent runs all cleanup tasks in parallel using goroutines.
// Collects errors via channel and waits for completion with WaitGroup.
// Significantly faster for independent cleanup operations.
func (sm *Shutdown) executeConcurrent(events []namedCall, ctx context.Context) {
	var wg sync.WaitGroup
	errChan := make(chan error, len(events))
	for _, nc := range events {
		wg.Add(1)
		go func(task namedCall) {
			defer wg.Done()
			sm.log("running (concurrent): %s (priority: %d)", task.Name, task.Priority)
			errChan <- task.Fn(ctx)
		}(nc)
	}
	go func() { wg.Wait(); close(errChan) }()
	failed := 0
	for err := range errChan {
		if err != nil {
			failed++
			sm.recordError(err)
			var se *ShutdownError
			if errors.As(err, &se) {
				sm.log("task failed (concurrent): %s -> %v", se.Name, se.Err)
			}
		}
	}
	sm.statsMu.Lock()
	sm.stats.FailedEvents += failed
	sm.stats.CompletedEvents += len(events) - failed
	sm.statsMu.Unlock()
}

// log writes formatted message using the configured logger.
// No-op when logger is nil (safe default behavior).
// Uses Infof level for all shutdown-related messages.
func (sm *Shutdown) log(format string, v ...any) {
	if sm.logger != nil {
		sm.logger.Infof(format, v...)
	}
}

// IsShuttingDown reports whether shutdown has been initiated.
// Thread-safe via atomic boolean; useful for guarding late registrations.
// Returns true once the first shutdown trigger occurs.
func (sm *Shutdown) IsShuttingDown() bool {
	return sm.inShutdown.Load()
}

// recordError appends a failed task error to the stats structure.
// Thread-safe; used by both sequential and concurrent executors.
// Preserves full error chain including wrapped ShutdownError.
func (sm *Shutdown) recordError(err error) {
	sm.statsMu.Lock()
	defer sm.statsMu.Unlock()
	sm.stats.Errors = append(sm.stats.Errors, err)
}

// GetStats returns a deep copy of current shutdown statistics.
// Safe for concurrent reads; protects against mutation during access.
// Used by public APIs to return final results.
// Used by public APIs to return final results.
func (sm *Shutdown) GetStats() *ShutdownStats {
	sm.statsMu.RLock()
	defer sm.statsMu.RUnlock()
	c := *sm.stats
	if sm.stats.Errors != nil {
		c.Errors = make([]error, len(sm.stats.Errors))
		copy(c.Errors, sm.stats.Errors)
	}
	return &c
}

// Done returns a channel closed when shutdown completes.
func (sm *Shutdown) Done() <-chan struct{} { return sm.doneChan }

// ShutdownError provides structured error details for a failed cleanup task.
type ShutdownError struct {
	Name      string
	Err       error
	Timestamp time.Time
}

// Error implements the error interface, combining the task name and underlying cause.
func (e *ShutdownError) Error() string { return fmt.Sprintf("%s: %v", e.Name, e.Err) }

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *ShutdownError) Unwrap() error { return e.Err }

// autoName derives a display name for an anonymous shutdown callback.
func autoName(fn any) string {
	val := reflect.ValueOf(fn)
	if val.Kind() == reflect.Func {
		if name := runtime.FuncForPC(val.Pointer()).Name(); name != "" {
			return name
		}
	}
	return fmt.Sprintf("anon-%s", ulid.Make().String())
}

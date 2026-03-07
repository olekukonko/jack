package jack

import (
	"container/heap"
	"context"
	"fmt"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

// PatientState represents the health state of a monitored patient.
type PatientState int

const (
	// PatientUnknown is the initial state before the first check completes.
	PatientUnknown PatientState = iota
	PatientHealthy
	PatientDegraded
	PatientFailed
)

// PatientEvent is emitted for observability when a patient's state changes.
type PatientEvent struct {
	ID        string
	State     PatientState
	LastCheck time.Time
	Duration  time.Duration
	Error     error
	Meta      map[string]any
}

// PatientConfig defines monitoring parameters for a single patient.
type PatientConfig struct {
	ID            string
	Interval      time.Duration      // Normal check interval (default: 10s)
	Jitter        float64            // 0.0–1.0, applied as ±percentage of interval
	Timeout       time.Duration      // Max duration per check (default: 30s)
	Accelerated   time.Duration      // Interval when manually degraded (0 = disabled)
	MaxFailures   uint64             // Consecutive failures before PatientFailed (0 = unlimited)
	Check         FuncCtx            // Required: the health check function
	OnStart       FuncCtx            // Called before each check (async, best-effort)
	OnComplete    FuncCtx            // Called after a successful check (async, best-effort)
	OnError       FuncCtx            // Called after a failed check (async, best-effort)
	OnTimeout     FuncCtx            // Called when a check times out (async, best-effort)
	OnRecover     FuncCtx            // Called when patient recovers (async, best-effort)
	OnStateChange func(PatientEvent) // Called synchronously on state change; keep lightweight
}

// storedError wraps an error so atomic.Value always sees a consistent concrete type.
type storedError struct{ err error }

// Patient holds the runtime state for one monitored entity.
// cfg is immutable after Add(); everything else is written atomically or from the scheduler goroutine.
type Patient struct {
	cfg            PatientConfig
	immediateStart bool // true when original Interval was <=0; first check fires immediately

	state          atomic.Value  // PatientState
	failures       atomic.Uint64 // consecutive failure count
	successes      atomic.Uint64 // consecutive success count (for Failed->Healthy threshold)
	manualDegraded atomic.Bool   // set by SetDegraded(true); suppresses auto-recovery

	lastCheck atomic.Value // time.Time
	lastError atomic.Value // storedError

	// nextRun and heapIndex are written only by the scheduler goroutine (single writer).
	nextRun   time.Time
	heapIndex int // -1 when not in heap

	// removed is set atomically when Remove()/StopAll() evicts this patient.
	removed atomic.Bool

	doctor *Doctor
}

// NewPatient creates a Patient. Call Doctor.Add() to begin monitoring.
func NewPatient(cfg PatientConfig) *Patient {
	immediate := cfg.Interval <= 0
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.Jitter < 0 || cfg.Jitter > 1 {
		cfg.Jitter = 0
	}
	return &Patient{
		cfg:            cfg,
		immediateStart: immediate,
		heapIndex:      -1,
	}
}

// ─── min-heap of patients ordered by nextRun ────────────────────────────────

type patientHeap []*Patient

func (h patientHeap) Len() int           { return len(h) }
func (h patientHeap) Less(i, j int) bool { return h[i].nextRun.Before(h[j].nextRun) }
func (h patientHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}
func (h *patientHeap) Push(x any) {
	p := x.(*Patient)
	p.heapIndex = len(*h)
	*h = append(*h, p)
}
func (h *patientHeap) Pop() any {
	old := *h
	n := len(old)
	p := old[n-1]
	old[n-1] = nil
	p.heapIndex = -1
	*h = old[:n-1]
	return p
}

// ─── Doctor ─────────────────────────────────────────────────────────────────

// Doctor monitors an arbitrary number of patients using exactly:
//   - 1 scheduler goroutine (drives all timing via a min-heap)
//   - N pool worker goroutines (execute checks concurrently, bounded)
//
// This is O(1) goroutines with respect to patient count, making it
// suitable for monitoring millions of endpoints.
type Doctor struct {
	pool          *Pool
	observable    Observable[PatientEvent]
	globalTimeout time.Duration
	maxConcurrent int
	logger        *ll.Logger

	// patientsMu guards the patients map; read-heavy so RWMutex.
	patientsMu sync.RWMutex
	patients   map[string]*Patient

	// mu guards the heap; only the scheduler goroutine writes to the heap,
	// but Add/SetDegraded push to it under this lock.
	mu     sync.Mutex
	pq     patientHeap
	wakeCh chan struct{} // buffered(1): signals scheduler that heap changed

	stopped atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

type DoctorOption func(*Doctor)

func DoctorWithPool(pool *Pool) DoctorOption {
	return func(d *Doctor) {
		if pool != nil {
			d.pool = pool
		}
	}
}

func DoctorWithObservable(obs Observable[PatientEvent]) DoctorOption {
	return func(d *Doctor) {
		if obs != nil {
			d.observable = obs
		}
	}
}

func DoctorWithMaxConcurrent(n int) DoctorOption {
	return func(d *Doctor) {
		if n > 0 {
			d.maxConcurrent = n
		}
	}
}

func DoctorWithGlobalTimeout(t time.Duration) DoctorOption {
	return func(d *Doctor) {
		if t > 0 {
			d.globalTimeout = t
		}
	}
}

func DoctorWithLogger(l *ll.Logger) DoctorOption {
	return func(d *Doctor) {
		if l != nil {
			d.logger = l.Namespace("doctor")
		}
	}
}

// NewDoctor creates a Doctor. The scheduler goroutine starts immediately.
func NewDoctor(opts ...DoctorOption) *Doctor {
	d := &Doctor{
		maxConcurrent: 50,
		globalTimeout: 30 * time.Second,
		patients:      make(map[string]*Patient),
		wakeCh:        make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		logger:        logger.Namespace("doctor"),
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.pool == nil {
		d.pool = NewPool(d.maxConcurrent)
	}
	heap.Init(&d.pq)
	d.wg.Add(1)
	go d.scheduler()
	return d
}

// Add registers (or replaces) a patient and begins monitoring immediately.
// Thread-safe; O(log n) on the heap.
func (d *Doctor) Add(p *Patient) error {
	if p.cfg.Check == nil {
		return fmt.Errorf("patient %s: Check function required", p.cfg.ID)
	}
	if p.cfg.Timeout <= 0 {
		p.cfg.Timeout = d.globalTimeout
	}
	p.doctor = d
	p.state.Store(PatientUnknown)
	p.lastCheck.Store(time.Time{})
	p.removed.Store(false)
	p.heapIndex = -1

	// Evict existing patient with same ID.
	d.patientsMu.Lock()
	if old, ok := d.patients[p.cfg.ID]; ok {
		old.removed.Store(true)
	}
	d.patients[p.cfg.ID] = p
	d.patientsMu.Unlock()

	// Compute first run time.
	delay := p.jitteredInterval(p.cfg.Interval)
	if p.immediateStart {
		delay = 0
	}
	p.nextRun = time.Now().Add(delay)

	d.mu.Lock()
	heap.Push(&d.pq, p)
	d.mu.Unlock()
	d.wake()
	return nil
}

// Remove stops monitoring and removes a patient. Thread-safe.
func (d *Doctor) Remove(id string) bool {
	d.patientsMu.Lock()
	p, ok := d.patients[id]
	if ok {
		delete(d.patients, id)
		p.removed.Store(true)
	}
	d.patientsMu.Unlock()
	return ok
}

// SetDegraded manually marks a patient degraded (accelerated checks) or recovers it.
func (d *Doctor) SetDegraded(id string, degraded bool) {
	d.patientsMu.RLock()
	p, ok := d.patients[id]
	d.patientsMu.RUnlock()
	if !ok {
		return
	}
	p.setDegraded(degraded, d)
}

// GetState returns the current state of a patient.
func (d *Doctor) GetState(id string) (PatientState, bool) {
	d.patientsMu.RLock()
	p, ok := d.patients[id]
	d.patientsMu.RUnlock()
	if !ok {
		return PatientUnknown, false
	}
	return p.state.Load().(PatientState), true
}

// StopAll shuts down monitoring and the pool gracefully.
func (d *Doctor) StopAll(timeout time.Duration) {
	if !d.stopped.CompareAndSwap(false, true) {
		return
	}
	close(d.stopCh)
	d.wg.Wait()

	d.patientsMu.Lock()
	for _, p := range d.patients {
		p.removed.Store(true)
	}
	d.patientsMu.Unlock()

	if d.pool != nil {
		_ = d.pool.Shutdown(timeout)
	}
}

// ─── scheduler ──────────────────────────────────────────────────────────────

// scheduler is the single goroutine that owns the min-heap.
// It sleeps until the next patient is due, then fires a non-blocking
// submitCheck() into the pool and goes back to sleep.
// Check execution never blocks the scheduler.
func (d *Doctor) scheduler() {
	defer d.wg.Done()
	for {
		d.mu.Lock()

		// Purge removed patients from the heap top.
		for d.pq.Len() > 0 && d.pq[0].removed.Load() {
			heap.Pop(&d.pq)
		}

		if d.pq.Len() == 0 {
			d.mu.Unlock()
			select {
			case <-d.stopCh:
				return
			case <-d.wakeCh:
			}
			continue
		}

		nextPatient := d.pq[0]
		wait := time.Until(nextPatient.nextRun)
		d.mu.Unlock()

		if wait > 0 {
			select {
			case <-d.stopCh:
				return
			case <-d.wakeCh:
				continue // heap may have changed; re-evaluate
			case <-time.After(wait):
			}
		}

		// Pop due patient and dispatch.
		d.mu.Lock()
		if d.pq.Len() == 0 {
			d.mu.Unlock()
			continue
		}
		p := heap.Pop(&d.pq).(*Patient)
		d.mu.Unlock()

		if p.removed.Load() {
			continue
		}
		d.submitCheck(p)
	}
}

func (d *Doctor) wake() {
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

// reschedule re-inserts a patient after its check completes.
// Called from pool worker goroutines via defer — not from scheduler.
func (d *Doctor) reschedule(p *Patient) {
	if p.removed.Load() {
		return
	}
	p.nextRun = time.Now().Add(p.effectiveInterval())
	d.mu.Lock()
	heap.Push(&d.pq, p)
	d.mu.Unlock()
	d.wake()
}

// submitCheck fires a patient's check into the pool. Never blocks the scheduler.
func (d *Doctor) submitCheck(p *Patient) {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.Timeout)
	_ = d.pool.SubmitCtx(ctx, FuncCtx(func(execCtx context.Context) error {
		defer cancel()
		defer func() { d.reschedule(p) }()

		if p.removed.Load() {
			return nil
		}

		cfg := p.cfg
		start := time.Now()

		if cfg.OnStart != nil {
			_ = d.pool.SubmitCtx(execCtx, cfg.OnStart)
		}

		err := func() (e error) {
			defer func() {
				if r := recover(); r != nil {
					e = fmt.Errorf("check panicked: %v", r)
				}
			}()
			return cfg.Check(execCtx)
		}()

		// Treat context expiry as an error even if Check returned nil.
		if execCtx.Err() != nil && err == nil {
			err = execCtx.Err()
		}

		duration := time.Since(start)
		p.lastCheck.Store(start)

		if err != nil {
			p.handleFailure(err, cfg)
		} else {
			p.handleSuccess(cfg)
		}

		if d.observable != nil {
			var lastErr error
			if v := p.lastError.Load(); v != nil {
				if se, ok := v.(storedError); ok {
					lastErr = se.err
				}
			}
			d.observable.Notify(PatientEvent{
				ID:        cfg.ID,
				State:     p.state.Load().(PatientState),
				LastCheck: start,
				Duration:  duration,
				Error:     lastErr,
			})
		}
		return nil
	}))
}

// ─── Patient state machine ───────────────────────────────────────────────────

func (p *Patient) handleFailure(err error, cfg PatientConfig) {
	p.lastError.Store(storedError{err: err})
	p.successes.Store(0)
	failures := p.failures.Add(1)

	if cfg.OnError != nil {
		_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnError)
	}
	if cfg.OnTimeout != nil && (err == context.DeadlineExceeded || err == context.Canceled) {
		_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnTimeout)
	}

	oldState := p.state.Load().(PatientState)
	if cfg.MaxFailures > 0 && failures >= cfg.MaxFailures && oldState != PatientFailed {
		p.state.Store(PatientFailed)
		p.emitStateChange(oldState, PatientFailed, err)
	} else if oldState == PatientHealthy || oldState == PatientUnknown {
		p.state.Store(PatientDegraded)
		p.emitStateChange(oldState, PatientDegraded, err)
	}
}

func (p *Patient) handleSuccess(cfg PatientConfig) {
	p.failures.Store(0)

	if cfg.OnComplete != nil {
		_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnComplete)
	}

	oldState := p.state.Load().(PatientState)
	switch oldState {
	case PatientFailed:
		// Require max(MaxFailures, 2) consecutive successes to leave Failed.
		minRec := cfg.MaxFailures
		if minRec < 2 {
			minRec = 2
		}
		if p.successes.Add(1) >= minRec {
			p.successes.Store(0)
			p.state.Store(PatientHealthy)
			p.emitStateChange(oldState, PatientHealthy, nil)
			if cfg.OnRecover != nil {
				_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnRecover)
			}
		}
	case PatientDegraded:
		if !p.manualDegraded.Load() {
			p.successes.Store(0)
			p.state.Store(PatientHealthy)
			p.emitStateChange(oldState, PatientHealthy, nil)
			if cfg.OnRecover != nil {
				_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnRecover)
			}
		}
		// If manualDegraded: stay Degraded, keep firing at Accelerated interval.
	case PatientUnknown:
		p.state.Store(PatientHealthy)
		p.emitStateChange(oldState, PatientHealthy, nil)
	}
}

func (p *Patient) setDegraded(degraded bool, d *Doctor) {
	oldState := p.state.Load().(PatientState)
	if degraded {
		if oldState == PatientHealthy || oldState == PatientUnknown {
			p.successes.Store(0)
			p.manualDegraded.Store(true)
			p.state.Store(PatientDegraded)
			p.emitStateChange(oldState, PatientDegraded, nil)
			d.mu.Lock()
			p.nextRun = time.Now()
			if p.heapIndex >= 0 {
				heap.Fix(&d.pq, p.heapIndex)
			} else if !p.removed.Load() {
				heap.Push(&d.pq, p)
			}
			d.mu.Unlock()
			d.wake()
		}
	} else {
		if oldState != PatientHealthy {
			p.manualDegraded.Store(false)
			p.failures.Store(0)
			p.successes.Store(0)
			p.state.Store(PatientHealthy)
			p.emitStateChange(oldState, PatientHealthy, nil)
			if p.cfg.OnRecover != nil {
				_ = d.pool.SubmitCtx(context.Background(), p.cfg.OnRecover)
			}
		}
	}
}

func (p *Patient) emitStateChange(old, new PatientState, err error) {
	if p.cfg.OnStateChange != nil {
		p.cfg.OnStateChange(PatientEvent{
			ID:    p.cfg.ID,
			State: new,
			Error: err,
			Meta:  map[string]any{"previous_state": old},
		})
	}
}

func (p *Patient) effectiveInterval() time.Duration {
	base := p.cfg.Interval
	if p.state.Load().(PatientState) == PatientDegraded && p.cfg.Accelerated > 0 {
		base = p.cfg.Accelerated
	}
	return p.jitteredInterval(base)
}

func (p *Patient) jitteredInterval(base time.Duration) time.Duration {
	if p.cfg.Jitter <= 0 {
		return base
	}
	factor := 1.0 + (mrand.Float64()*2-1.0)*p.cfg.Jitter
	return time.Duration(float64(base) * factor)
}

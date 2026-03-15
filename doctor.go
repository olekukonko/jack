package jack

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

type storedError struct{ err error }

// DoctorMetrics tracks global operational metrics across all patients.
type DoctorMetrics struct {
	PatientsTotal     atomic.Int64
	ChecksTotal       atomic.Uint64
	ChecksHealthy     atomic.Uint64
	ChecksDegraded    atomic.Uint64
	ChecksFailed      atomic.Uint64
	StateChanges      atomic.Uint64
	AcceleratedChecks atomic.Uint64
	ManualDegraded    atomic.Uint64
	Recoveries        atomic.Uint64
	Timeouts          atomic.Uint64
	PanicsRecovered   atomic.Uint64
	PoolSubmits       atomic.Uint64
	PoolSubmitFails   atomic.Uint64
}

// Doctor monitors patients with global and per-patient metrics.
type Doctor struct {
	pool          *Pool
	observable    Observable[PatientEvent]
	globalTimeout time.Duration
	maxConcurrent int
	logger        *ll.Logger
	metrics       *DoctorMetrics

	patientsMu sync.RWMutex
	patients   map[string]*Patient

	mu     sync.Mutex
	pq     patientHeap
	wakeCh chan struct{}

	stopped atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	verbose atomic.Bool
}

type DoctorOption func(*Doctor)

// DoctorWithPool configures the Doctor to use a custom worker pool for check execution.
// If the provided pool is nil, the option has no effect and default pool creation applies.
// Use this to control concurrency limits and resource sharing across multiple Doctors.
func DoctorWithPool(pool *Pool) DoctorOption {
	return func(d *Doctor) {
		if pool != nil {
			d.pool = pool
		}
	}
}

// DoctorWithObservable attaches an event observer to receive patient state change notifications.
// The observable receives PatientEvent structs whenever a patient's health status updates.
// Use this for external monitoring, logging, or triggering downstream actions based on health.
func DoctorWithObservable(obs Observable[PatientEvent]) DoctorOption {
	return func(d *Doctor) {
		if obs != nil {
			d.observable = obs
		}
	}
}

// DoctorWithMaxConcurrent sets the maximum number of concurrent health checks the Doctor can run.
// If the provided value is zero or negative, the option is ignored and the default applies.
// This limit controls resource usage and prevents system overload during intensive monitoring.
func DoctorWithMaxConcurrent(n int) DoctorOption {
	return func(d *Doctor) {
		if n > 0 {
			d.maxConcurrent = n
		}
	}
}

// DoctorWithGlobalTimeout configures the default timeout for patient checks lacking individual settings.
// If the provided duration is zero or negative, the option is ignored and the default applies.
// This ensures checks do not hang indefinitely and helps maintain responsive health monitoring.
func DoctorWithGlobalTimeout(t time.Duration) DoctorOption {
	return func(d *Doctor) {
		if t > 0 {
			d.globalTimeout = t
		}
	}
}

// DoctorWithLogger assigns a namespaced logger instance for structured Doctor operation logging.
// If the provided logger is nil, the option has no effect and the default logger remains active.
// Use this to integrate Doctor logs with your application's logging infrastructure and levels.
func DoctorWithLogger(l *ll.Logger) DoctorOption {
	return func(d *Doctor) {
		if l != nil {
			d.logger = l.Namespace("doctor")
		}
	}
}

// DoctorWithVerbose enables or disables detailed debug logging for Doctor operations and checks.
// When enabled, additional context like durations and errors are logged for troubleshooting.
// Use this during development or debugging, and disable in production for reduced log volume.
func DoctorWithVerbose(v bool) DoctorOption {
	return func(d *Doctor) {
		d.verbose.Store(v)
	}
}

// NewDoctor creates and initializes a new Doctor instance with the provided configuration options.
// It sets up the patient priority queue, starts the scheduler goroutine, and creates a default pool if needed.
// The returned Doctor is immediately active and ready to accept patient registrations via Add.
func NewDoctor(opts ...DoctorOption) *Doctor {
	d := &Doctor{
		maxConcurrent: 50,
		globalTimeout: 30 * time.Second,
		patients:      make(map[string]*Patient),
		wakeCh:        make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		logger:        logger.Namespace("doctor"),
		metrics:       &DoctorMetrics{},
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.pool == nil {
		d.pool = NewPool(d.maxConcurrent)
		d.logger.Fields("maxConcurrent", d.maxConcurrent).Info("created default pool")
	}
	heap.Init(&d.pq)
	d.wg.Add(1)
	go d.scheduler()
	d.logger.Info("doctor started")
	return d
}

// Metrics returns a reference to the Doctor's global metrics struct for monitoring operational statistics.
// All metric fields use atomic operations, allowing safe concurrent reads without additional locking.
// Use this to expose health check statistics to dashboards, alerts, or external monitoring systems.
func (d *Doctor) Metrics() *DoctorMetrics {
	return d.metrics
}

// Add registers a new patient with the Doctor, validating its configuration and scheduling its first check.
// If a patient with the same ID already exists, it is replaced and the old instance is marked removed.
// Returns an error if the patient lacks a required Check function or if registration fails.
func (d *Doctor) Add(p *Patient) error {
	if p.cfg.Check == nil {
		d.logger.Fields("patientID", p.cfg.ID).Error("add failed: Check function required")
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

	d.patientsMu.Lock()
	if old, ok := d.patients[p.cfg.ID]; ok {
		old.removed.Store(true)
		d.logger.Fields("patientID", p.cfg.ID).Warn("replaced existing patient")
	}
	d.patients[p.cfg.ID] = p
	d.metrics.PatientsTotal.Store(int64(len(d.patients)))
	d.patientsMu.Unlock()

	delay := p.jitteredInterval(p.cfg.Interval)
	if p.immediateStart {
		delay = 0
	}
	p.nextRun = time.Now().Add(delay)

	d.mu.Lock()
	heap.Push(&d.pq, p)
	d.mu.Unlock()
	d.wake()

	d.logger.Fields("patientID", p.cfg.ID, "interval", p.cfg.Interval, "delay", delay).Info("patient added")
	return nil
}

// Remove unregisters a patient by ID, marking it as removed and preventing future scheduled checks.
// The method returns true if the patient was found and successfully removed, false otherwise.
// Removed patients are cleaned from the scheduler queue on the next scheduling cycle.
func (d *Doctor) Remove(id string) bool {
	d.patientsMu.Lock()
	p, ok := d.patients[id]
	if ok {
		delete(d.patients, id)
		p.removed.Store(true)
		p.Remove()
		d.metrics.PatientsTotal.Store(int64(len(d.patients)))
		d.logger.Fields("patientID", id).Info("patient removed")
	}
	d.patientsMu.Unlock()
	return ok
}

// Stop halts a specific patient by ID, marking it removed and preventing any further health checks.
// It returns true if the patient was found and stopped, false if the patient did not exist.
// This method is safe to call multiple times and ensures clean removal from the scheduler.
func (d *Doctor) Stop(id string) bool {
	d.patientsMu.RLock()
	p, ok := d.patients[id]
	d.patientsMu.RUnlock()

	if !ok {
		d.logger.Fields("patientID", id).Warn("stop failed: not found")
		return false
	}

	if p.removed.Load() {
		d.logger.Fields("patientID", id).Warn("stop failed: already removed")
		return false
	}

	p.removed.Store(true)
	p.Remove()

	d.patientsMu.Lock()
	delete(d.patients, id)
	d.metrics.PatientsTotal.Store(int64(len(d.patients)))
	d.patientsMu.Unlock()

	d.wake()
	d.logger.Fields("patientID", id).Info("patient stopped")
	return true
}

// SetDegraded manually overrides a patient's degradation state, forcing degraded or healthy status.
// If the patient ID is not found, the method logs a warning and returns without making changes.
// Use this for operational control, such as forcing maintenance mode or acknowledging known issues.
func (d *Doctor) SetDegraded(id string, degraded bool) {
	d.patientsMu.RLock()
	p, ok := d.patients[id]
	d.patientsMu.RUnlock()
	if !ok {
		d.logger.Fields("patientID", id).Warn("SetDegraded failed: not found")
		return
	}
	p.setDegraded(degraded, d)
}

// GetState retrieves the current health state of a patient by ID along with an existence indicator.
// If the patient is not registered, it returns PatientUnknown and false for the existence flag.
// This method provides thread-safe read access to patient state without modifying any internal data.
func (d *Doctor) GetState(id string) (PatientState, bool) {
	d.patientsMu.RLock()
	p, ok := d.patients[id]
	d.patientsMu.RUnlock()
	if !ok {
		return PatientUnknown, false
	}
	return p.state.Load().(PatientState), true
}

// StopAll gracefully shuts down the Doctor, stopping all patients and the scheduler goroutine.
// It uses an atomic flag to ensure idempotency and waits for in-flight operations with the given timeout.
// After calling StopAll, the Doctor cannot be restarted and all patient registrations are cleared.
func (d *Doctor) StopAll(timeout time.Duration) {
	if !d.stopped.CompareAndSwap(false, true) {
		d.logger.Warn("StopAll: already stopped")
		return
	}
	d.logger.Fields("timeout", timeout).Info("stopping all patients")
	close(d.stopCh)
	d.wg.Wait()

	d.patientsMu.Lock()
	count := len(d.patients)
	for _, p := range d.patients {
		p.removed.Store(true)
	}
	d.patientsMu.Unlock()

	if d.pool != nil {
		_ = d.pool.Shutdown(timeout)
	}
	d.logger.Fields("count", count).Info("all patients stopped")
}

// scheduler runs the main event loop that manages patient check scheduling based on priority and timing.
// It efficiently waits for the next due patient, handles wake signals, and submits checks to the worker pool.
// The loop terminates cleanly when the stop channel is closed, ensuring no goroutine leaks.
func (d *Doctor) scheduler() {
	defer d.wg.Done()
	for {
		d.mu.Lock()

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
				continue
			case <-time.After(wait):
			}
		}

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

// wake sends a non-blocking signal to the scheduler to re-evaluate the patient queue immediately.
// It uses a buffered channel with select-default to avoid blocking the caller if the signal is pending.
// This method is called after queue modifications to ensure timely processing of urgent checks.
func (d *Doctor) wake() {
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

// reschedule calculates the next run time for a patient and re-inserts it into the priority queue.
// It respects the patient's removal flag to avoid rescheduling patients that have been unregistered.
// After updating the queue, it signals the scheduler to wake and process the updated schedule.
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

// submitCheck executes a patient's health check with timeout, panic recovery, and comprehensive metrics tracking.
// It handles success and failure paths, updates rolling averages atomically, and notifies observers of state changes.
// The check runs in the worker pool with proper context cancellation and ensures the patient is rescheduled afterward.
func (d *Doctor) submitCheck(p *Patient) {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.Timeout)
	submitted := d.pool.SubmitCtx(ctx, FuncCtx(func(execCtx context.Context) error {
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
					d.metrics.PanicsRecovered.Add(1)
					p.metrics.PanicsRecovered.Add(1)
					d.logger.Fields("patientID", cfg.ID, "panic", r).Error("check panicked")
				}
			}()
			return cfg.Check(execCtx)
		}()

		if execCtx.Err() != nil && err == nil {
			err = execCtx.Err()
		}

		duration := time.Since(start)
		checkMs := int64(duration.Milliseconds())

		// Load total once to avoid race in rolling average calculation
		total := d.metrics.ChecksTotal.Load()
		d.metrics.ChecksTotal.Add(1)
		p.metrics.ChecksTotal.Add(1)
		p.metrics.LastCheckMs.Store(checkMs)

		// Update rolling average atomically
		for {
			oldAvg := p.metrics.AvgCheckMs.Load()
			var newAvg int64
			if total == 0 {
				newAvg = checkMs
			} else {
				newAvg = (oldAvg*int64(total) + checkMs) / int64(total+1)
			}
			if p.metrics.AvgCheckMs.CompareAndSwap(oldAvg, newAvg) {
				break
			}
		}

		p.lastCheck.Store(start)

		if err != nil {
			d.metrics.ChecksFailed.Add(1)
			p.metrics.ChecksFailed.Add(1)
			p.metrics.ConsecFailures.Add(1)
			p.metrics.ConsecSuccesses.Store(0)

			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				d.metrics.Timeouts.Add(1)
				p.metrics.Timeouts.Add(1)

				if d.verbose.Load() {
					d.logger.Fields("patientID", cfg.ID, "duration", duration).Warn("check timeout")
				}

			} else {
				if d.verbose.Load() {
					d.logger.Fields("patientID", cfg.ID, "duration", duration, "error", err).Error("check failed")
				}
			}
			p.handleFailure(err, cfg)
		} else {
			d.metrics.ChecksHealthy.Add(1)
			p.metrics.ChecksHealthy.Add(1)
			p.metrics.ConsecSuccesses.Add(1)
			p.metrics.ConsecFailures.Store(0)
			p.handleSuccess(cfg)
			if d.verbose.Load() {
				d.logger.Fields("patientID", cfg.ID, "duration", duration).Debug("check healthy")
			}
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

	if submitted != nil {
		d.metrics.PoolSubmitFails.Add(1)
		d.logger.Fields("patientID", p.cfg.ID, "error", submitted).Error("pool submit failed")
	} else {
		d.metrics.PoolSubmits.Add(1)
	}
}

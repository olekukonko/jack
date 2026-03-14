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

func DoctorWithVerbose(v bool) DoctorOption {
	return func(d *Doctor) {
		d.verbose.Store(v)
	}
}

// NewDoctor creates a Doctor with global metrics.
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

// Metrics returns the doctor's global metrics.
func (d *Doctor) Metrics() *DoctorMetrics {
	return d.metrics
}

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

// Stop stops a single patient by ID. Returns true if found and stopped.
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

func (d *Doctor) GetState(id string) (PatientState, bool) {
	d.patientsMu.RLock()
	p, ok := d.patients[id]
	d.patientsMu.RUnlock()
	if !ok {
		return PatientUnknown, false
	}
	return p.state.Load().(PatientState), true
}

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

func (d *Doctor) wake() {
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

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

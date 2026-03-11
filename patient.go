package jack

import (
	"container/heap"
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// PatientState represents the health state of a monitored patient.
type PatientState int

const (
	PatientUnknown PatientState = iota
	PatientHealthy
	PatientDegraded
	PatientFailed
)

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

// PatientEvent is emitted for observability when a patient's state changes.
type PatientEvent struct {
	ID        string
	State     PatientState
	LastCheck time.Time
	Duration  time.Duration
	Error     error
	Meta      map[string]any
}

// PatientMetrics tracks per-patient operational metrics.
type PatientMetrics struct {
	ChecksTotal     atomic.Uint64
	ChecksHealthy   atomic.Uint64
	ChecksDegraded  atomic.Uint64
	ChecksFailed    atomic.Uint64
	StateChanges    atomic.Uint64
	Recoveries      atomic.Uint64
	Timeouts        atomic.Uint64
	PanicsRecovered atomic.Uint64
	ConsecFailures  atomic.Int64
	ConsecSuccesses atomic.Int64
	LastCheckMs     atomic.Int64
	AvgCheckMs      atomic.Int64
	LastFailureMs   atomic.Int64
	LastRecoveryMs  atomic.Int64
}

// PatientConfig defines monitoring parameters for a single patient.
type PatientConfig struct {
	ID            string
	Interval      time.Duration
	Jitter        float64
	Timeout       time.Duration
	Accelerated   time.Duration
	MaxFailures   uint64
	Check         FuncCtx
	OnStart       FuncCtx
	OnComplete    FuncCtx
	OnError       FuncCtx
	OnTimeout     FuncCtx
	OnRecover     FuncCtx
	OnRemove      func()
	OnStateChange func(PatientEvent)
}

// Patient holds runtime state for one monitored entity with its own metrics.
type Patient struct {
	cfg            PatientConfig
	immediateStart bool
	state          atomic.Value
	failures       atomic.Uint64
	successes      atomic.Uint64
	manualDegraded atomic.Bool
	lastCheck      atomic.Value
	lastError      atomic.Value
	nextRun        time.Time
	heapIndex      int
	removed        atomic.Bool
	doctor         *Doctor
	metrics        *PatientMetrics
}

// NewPatient creates a Patient with its own metrics instance.
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
		metrics:        &PatientMetrics{},
	}
}

// Metrics returns the patient's individual metrics.
func (p *Patient) Metrics() *PatientMetrics {
	return p.metrics
}

// Remove signals that this patient has been removed from monitoring.
// It invokes the OnRemove callback if configured.
func (p *Patient) Remove() {
	if p.removed.CompareAndSwap(false, true) {
		if p.cfg.OnRemove != nil {
			p.cfg.OnRemove()
		}
	}
}

func (p *Patient) handleFailure(err error, cfg PatientConfig) {
	p.lastError.Store(storedError{err: err})
	p.successes.Store(0)
	failures := p.failures.Add(1)

	if cfg.OnError != nil {
		_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnError)
	}
	if cfg.OnTimeout != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
		_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnTimeout)
	}

	oldState := p.state.Load().(PatientState)
	if cfg.MaxFailures > 0 && failures >= cfg.MaxFailures && oldState != PatientFailed {
		p.state.Store(PatientFailed)
		p.metrics.ChecksFailed.Add(1)
		p.metrics.StateChanges.Add(1)
		p.doctor.metrics.ChecksFailed.Add(1)
		p.doctor.metrics.StateChanges.Add(1)
		p.emitStateChange(oldState, PatientFailed, err)
	} else if oldState == PatientHealthy || oldState == PatientUnknown {
		p.state.Store(PatientDegraded)
		p.metrics.ChecksDegraded.Add(1)
		p.metrics.StateChanges.Add(1)
		p.doctor.metrics.ChecksDegraded.Add(1)
		p.doctor.metrics.StateChanges.Add(1)
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
		minRec := cfg.MaxFailures
		if minRec < 2 {
			minRec = 2
		}
		if p.successes.Add(1) >= minRec {
			p.successes.Store(0)
			p.state.Store(PatientHealthy)
			p.metrics.Recoveries.Add(1)
			p.metrics.StateChanges.Add(1)
			p.metrics.LastRecoveryMs.Store(time.Now().UnixMilli())
			p.doctor.metrics.Recoveries.Add(1)
			p.doctor.metrics.StateChanges.Add(1)
			p.emitStateChange(oldState, PatientHealthy, nil)
			if cfg.OnRecover != nil {
				_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnRecover)
			}
		}
	case PatientDegraded:
		if !p.manualDegraded.Load() {
			p.successes.Store(0)
			p.state.Store(PatientHealthy)
			p.metrics.Recoveries.Add(1)
			p.metrics.StateChanges.Add(1)
			p.metrics.LastRecoveryMs.Store(time.Now().UnixMilli())
			p.doctor.metrics.Recoveries.Add(1)
			p.doctor.metrics.StateChanges.Add(1)
			p.emitStateChange(oldState, PatientHealthy, nil)
			if cfg.OnRecover != nil {
				_ = p.doctor.pool.SubmitCtx(context.Background(), cfg.OnRecover)
			}
		}
	case PatientUnknown:
		p.state.Store(PatientHealthy)
		p.metrics.StateChanges.Add(1)
		p.doctor.metrics.StateChanges.Add(1)
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
			p.metrics.StateChanges.Add(1)
			d.metrics.ManualDegraded.Add(1)
			d.metrics.StateChanges.Add(1)
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
			p.metrics.StateChanges.Add(1)
			p.metrics.LastRecoveryMs.Store(time.Now().UnixMilli())
			d.metrics.StateChanges.Add(1)
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
		p.doctor.metrics.AcceleratedChecks.Add(1)
		base = p.cfg.Accelerated
	}
	return p.jitteredInterval(base)
}

func (p *Patient) jitteredInterval(base time.Duration) time.Duration {
	if p.cfg.Jitter <= 0 {
		return base
	}
	factor := 1.0 + (rand.Float64()*2-1.0)*p.cfg.Jitter
	return time.Duration(float64(base) * factor)
}

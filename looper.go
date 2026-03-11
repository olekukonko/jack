package jack

import (
	"context"
	"math"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

type LoopConfig struct {
	Name        string
	Interval    time.Duration
	Jitter      float64
	Backoff     bool
	MaxInterval time.Duration
	MinInterval time.Duration
	Immediate   bool
	Context     context.Context
	Logger      *ll.Logger
}

func (c *LoopConfig) validate() {
	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	if c.Jitter < 0 {
		c.Jitter = 0
	}
	if c.Jitter > 1 {
		c.Jitter = 1
	}
	if c.MaxInterval > 0 && c.MaxInterval < c.Interval {
		c.MaxInterval = c.Interval
	}
	if c.MinInterval > 0 && c.MinInterval > c.Interval {
		c.MinInterval = c.Interval
	}
}

// LooperMetrics tracks comprehensive operational metrics.
type LooperMetrics struct {
	Executions      atomic.Uint64
	Failures        atomic.Uint64
	Successes       atomic.Uint64
	BackoffEvents   atomic.Uint64
	IntervalChanges atomic.Uint64
	PanicsRecovered atomic.Uint64
	ContextCancels  atomic.Uint64
	Timeouts        atomic.Uint64

	LastRun   atomic.Value
	LastError atomic.Value
	LastStart atomic.Int64
	LastEnd   atomic.Int64

	TotalDurationNs atomic.Int64
	MinDurationNs   atomic.Int64
	MaxDurationNs   atomic.Int64

	CurrentIntervalNs atomic.Int64
	CurrentBackoffNs  atomic.Int64
}

func (m *LooperMetrics) recordExecution(start time.Time, err error) {
	m.Executions.Add(1)
	m.LastRun.Store(start)
	m.LastStart.Store(start.UnixNano())

	end := time.Now()
	m.LastEnd.Store(end.UnixNano())

	duration := end.Sub(start)
	durationNs := int64(duration)
	m.TotalDurationNs.Add(durationNs)

	// Min duration (0 means unset)
	for {
		current := m.MinDurationNs.Load()
		if current == 0 || durationNs < current {
			if m.MinDurationNs.CompareAndSwap(current, durationNs) {
				break
			}
		} else {
			break
		}
	}

	// Max duration
	for {
		current := m.MaxDurationNs.Load()
		if durationNs > current {
			if m.MaxDurationNs.CompareAndSwap(current, durationNs) {
				break
			}
		} else {
			break
		}
	}

	if err != nil {
		m.Failures.Add(1)
		m.LastError.Store(err)
	} else {
		m.Successes.Add(1)
	}
}

func (m *LooperMetrics) recordPanic() {
	m.PanicsRecovered.Add(1)
	m.LastError.Store(&CaughtPanic{Value: "panic in looper task"})
}

type Looper struct {
	config          LoopConfig
	task            func() error
	ctx             context.Context
	cancel          context.CancelFunc
	stopOnce        sync.Once
	wg              sync.WaitGroup
	mu              sync.RWMutex
	currentInterval atomic.Int64
	running         atomic.Bool
	failureCount    atomic.Uint64
	logger          *ll.Logger
	metrics         *LooperMetrics
	resetCh         chan struct{}
}

type LooperOption func(*LoopConfig)

func WithLooperName(name string) LooperOption {
	return func(c *LoopConfig) { c.Name = name }
}

func WithLooperInterval(interval time.Duration) LooperOption {
	return func(c *LoopConfig) {
		if interval > 0 {
			c.Interval = interval
		}
	}
}

func WithLooperJitter(jitter float64) LooperOption {
	return func(c *LoopConfig) { c.Jitter = jitter }
}

func WithLooperBackoff(enabled bool) LooperOption {
	return func(c *LoopConfig) { c.Backoff = enabled }
}

func WithLooperMaxInterval(max time.Duration) LooperOption {
	return func(c *LoopConfig) { c.MaxInterval = max }
}

func WithLooperMinInterval(min time.Duration) LooperOption {
	return func(c *LoopConfig) { c.MinInterval = min }
}

func WithLooperImmediate(immediate bool) LooperOption {
	return func(c *LoopConfig) { c.Immediate = immediate }
}

func WithLooperContext(ctx context.Context) LooperOption {
	return func(c *LoopConfig) { c.Context = ctx }
}

func WithLooperLogger(logger *ll.Logger) LooperOption {
	return func(c *LoopConfig) { c.Logger = logger }
}

// NewLooper creates a new Looper with full metrics.
func NewLooper(task Func, opts ...LooperOption) *Looper {
	if task == nil {
		panic("looper: task cannot be nil")
	}
	cfg := LoopConfig{
		Interval:  time.Second,
		Jitter:    0,
		Backoff:   false,
		Immediate: false,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.validate()
	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	logger := cfg.Logger
	if logger == nil {
		logger = ll.New("looper").Disable()
	}
	if cfg.Name != "" {
		logger = logger.Namespace(cfg.Name)
	}
	l := &Looper{
		config:  cfg,
		task:    task,
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		metrics: &LooperMetrics{},
		resetCh: make(chan struct{}, 1),
	}
	l.currentInterval.Store(int64(cfg.Interval))
	l.metrics.CurrentIntervalNs.Store(int64(cfg.Interval))
	return l
}

// Metrics returns the looper's metrics.
func (l *Looper) Metrics() *LooperMetrics {
	return l.metrics
}

func (l *Looper) Start() {
	if l.running.Swap(true) {
		return
	}
	l.logger.Info("looper starting (interval=%v, jitter=%.2f, backoff=%v)",
		l.config.Interval, l.config.Jitter, l.config.Backoff)
	if l.config.Immediate {
		l.execute()
	}
	l.wg.Add(1)
	go l.run()
}

func (l *Looper) Stop() {
	l.stopOnce.Do(func() {
		l.logger.Info("looper stopping")
		l.cancel()
		l.wg.Wait()
		l.running.Store(false)
		l.logger.Info("looper stopped")
	})
}

func (l *Looper) SetInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	old := l.currentInterval.Swap(int64(d))
	if old != int64(d) {
		l.metrics.IntervalChanges.Add(1)
		l.metrics.CurrentIntervalNs.Store(int64(d))
		l.logger.Debug("interval changed: %v -> %v", time.Duration(old), d)
		select {
		case l.resetCh <- struct{}{}:
		default:
		}
	}
}

func (l *Looper) ResetInterval() {
	l.SetInterval(l.config.Interval)
	l.metrics.CurrentBackoffNs.Store(0)
}

func (l *Looper) CurrentInterval() time.Duration {
	return time.Duration(l.currentInterval.Load())
}

func (l *Looper) IsRunning() bool {
	return l.running.Load()
}

func (l *Looper) FailureCount() uint64 {
	return l.failureCount.Load()
}

func (l *Looper) run() {
	defer l.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			l.logger.Error("looper panic: %v\n%s", r, debug.Stack())
		}
	}()
	if l.config.Immediate {
		l.execute()
	} else {
		l.sleep(l.calculateInterval(l.CurrentInterval()))
		l.execute()
	}
	ticker := time.NewTicker(l.calculateInterval(l.CurrentInterval()))
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			l.metrics.ContextCancels.Add(1)
			l.logger.Info("looper context done: %v", l.ctx.Err())
			return
		case <-ticker.C:
			l.execute()
			ticker.Reset(l.calculateInterval(l.CurrentInterval()))
		case <-l.resetCh:
			ticker.Reset(l.calculateInterval(l.CurrentInterval()))
		}
	}
}

func (l *Looper) execute() {
	start := time.Now()
	err := l.safeExecute()
	l.metrics.recordExecution(start, err)

	if err != nil {
		l.failureCount.Add(1)
		if l.config.Backoff {
			l.metrics.BackoffEvents.Add(1)
			l.applyBackoff()
		}
		l.logger.Warn("task failed (failures=%d): %v", l.failureCount.Load(), err)
	} else {
		if l.failureCount.Load() > 0 {
			l.failureCount.Store(0)
			l.ResetInterval()
			l.logger.Info("task recovered, interval reset to %v", l.CurrentInterval())
		}
	}
	l.logger.Debug("execution completed in %v", time.Since(start))
}

func (l *Looper) safeExecute() (err error) {
	defer func() {
		if r := recover(); r != nil {
			l.metrics.recordPanic()
			err = &CaughtPanic{
				Value: r,
				Stack: debug.Stack(),
			}
		}
	}()
	return l.task()
}

func (l *Looper) applyBackoff() {
	failures := l.failureCount.Load()
	multiplier := math.Pow(2, float64(failures-1))
	backoff := time.Duration(float64(l.config.Interval) * multiplier)
	if l.config.MaxInterval > 0 && backoff > l.config.MaxInterval {
		backoff = l.config.MaxInterval
	}
	if l.config.MinInterval > 0 && backoff < l.config.MinInterval {
		backoff = l.config.MinInterval
	}
	l.SetInterval(backoff)
	l.metrics.CurrentBackoffNs.Store(int64(backoff))
	l.logger.Debug("backoff applied: %v (failures=%d)", backoff, failures)
}

func (l *Looper) calculateInterval(base time.Duration) time.Duration {
	if l.config.Jitter <= 0 {
		return base
	}
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	factor := 1.0 + (rng.Float64()*2-1.0)*l.config.Jitter
	return time.Duration(float64(base) * factor)
}

func (l *Looper) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-l.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

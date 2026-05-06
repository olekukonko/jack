package jack

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveOption configures an AdaptiveLimiter.
type AdaptiveOption func(*AdaptiveLimiter)

// AdaptiveWithInitialLimit sets the starting concurrency limit (default 10).
func AdaptiveWithInitialLimit(n int) AdaptiveOption {
	return func(a *AdaptiveLimiter) {
		if n > 0 {
			a.initialLimit = n
		}
	}
}

// AdaptiveWithMinLimit sets the floor below which the limit will not drop (default 1).
func AdaptiveWithMinLimit(n int) AdaptiveOption {
	return func(a *AdaptiveLimiter) {
		if n > 0 {
			a.minLimit = n
		}
	}
}

// AdaptiveWithMaxLimit sets the ceiling above which the limit will not grow (default 200).
func AdaptiveWithMaxLimit(n int) AdaptiveOption {
	return func(a *AdaptiveLimiter) {
		if n > 0 {
			a.maxLimit = n
		}
	}
}

// AdaptiveWithTargetP50 sets the target median RTT the controller aims to maintain.
// When measured RTT exceeds this, the limit is reduced (default 100ms).
func AdaptiveWithTargetP50(d time.Duration) AdaptiveOption {
	return func(a *AdaptiveLimiter) {
		if d > 0 {
			a.targetRTT = d
		}
	}
}

// AdaptiveWithSmoothingFactor controls EWMA smoothing for RTT samples (default 0.1).
// Closer to 0 = slower adaptation; closer to 1 = reacts to every sample.
func AdaptiveWithSmoothingFactor(f float64) AdaptiveOption {
	return func(a *AdaptiveLimiter) {
		if f > 0 && f <= 1 {
			a.smoothing = f
		}
	}
}

// AdaptiveLimiterMetrics tracks adaptive limiter operational statistics.
type AdaptiveLimiterMetrics struct {
	Acquired        atomic.Uint64 // successful acquisitions
	Rejected        atomic.Uint64 // rejections when at limit
	LimitIncr       atomic.Uint64 // times the limit was increased
	LimitDecr       atomic.Uint64 // times the limit was decreased
	CurrentLimit    atomic.Int64  // live concurrency limit
	CurrentInFlight atomic.Int64  // goroutines currently executing
	AvgRTTNs        atomic.Int64  // EWMA RTT in nanoseconds
}

// AdaptiveLimiter adjusts its concurrency limit dynamically based on observed
// round-trip latency using a gradient-style AIMD controller.
//
// When RTT ≤ target → additive increase (limit++).
// When RTT > target → multiplicative decrease (limit × ratio).
//
// Composable with the rest of the library: the AdaptiveLimiter owns a
// Semaphore internally and exposes the same Call / Close interface as
// Breaker and Bulkhead.
type AdaptiveLimiter struct {
	sem          *Semaphore
	targetRTT    time.Duration
	smoothing    float64
	initialLimit int
	minLimit     int
	maxLimit     int

	limit    atomic.Int64 // current concurrency limit
	inFlight atomic.Int64
	ewmaRTT  atomic.Int64 // EWMA of RTT in nanoseconds

	mu      sync.Mutex // guards limit adjustment
	metrics *AdaptiveLimiterMetrics
}

// NewAdaptiveLimiter creates an AdaptiveLimiter with sensible defaults.
func NewAdaptiveLimiter(opts ...AdaptiveOption) *AdaptiveLimiter {
	a := &AdaptiveLimiter{
		targetRTT:    100 * time.Millisecond,
		smoothing:    0.1,
		initialLimit: 10,
		minLimit:     1,
		maxLimit:     200,
		metrics:      &AdaptiveLimiterMetrics{},
	}
	for _, opt := range opts {
		opt(a)
	}
	a.limit.Store(int64(a.initialLimit))
	a.sem = NewSemaphore(a.initialLimit)
	a.metrics.CurrentLimit.Store(int64(a.initialLimit))
	return a
}

// Metrics returns the limiter's operational metrics.
func (a *AdaptiveLimiter) Metrics() *AdaptiveLimiterMetrics { return a.metrics }

// Limit returns the current concurrency limit.
func (a *AdaptiveLimiter) Limit() int { return int(a.limit.Load()) }

// InFlight returns the number of concurrently executing calls.
func (a *AdaptiveLimiter) InFlight() int { return int(a.inFlight.Load()) }

// Call executes fn if a slot is available within the adaptive limit.
// It measures RTT and adjusts the limit after each call returns.
func (a *AdaptiveLimiter) Call(ctx context.Context, p Priority, fn func(context.Context) error) error {
	if err := a.sem.Acquire(ctx, p); err != nil {
		a.metrics.Rejected.Add(1)
		return err
	}
	a.inFlight.Add(1)
	a.metrics.CurrentInFlight.Store(a.inFlight.Load())

	start := time.Now()
	err := fn(ctx)
	rtt := time.Since(start)

	a.inFlight.Add(-1)
	a.metrics.CurrentInFlight.Store(a.inFlight.Load())
	a.sem.Release()
	a.metrics.Acquired.Add(1)

	a.updateRTT(rtt)
	a.adjust()

	return err
}

// Close shuts down the underlying semaphore, unblocking all pending callers.
func (a *AdaptiveLimiter) Close() { a.sem.Close() }

// updateRTT applies an EWMA update with the latest round-trip sample.
func (a *AdaptiveLimiter) updateRTT(rtt time.Duration) {
	ns := rtt.Nanoseconds()
	for {
		old := a.ewmaRTT.Load()
		var newVal int64
		if old == 0 {
			newVal = ns
		} else {
			newVal = int64(float64(old)*(1-a.smoothing) + float64(ns)*a.smoothing)
		}
		if a.ewmaRTT.CompareAndSwap(old, newVal) {
			a.metrics.AvgRTTNs.Store(newVal)
			break
		}
	}
}

// adjust computes a new concurrency limit using AIMD and applies it via resize.
func (a *AdaptiveLimiter) adjust() {
	a.mu.Lock()
	defer a.mu.Unlock()

	ewma := time.Duration(a.ewmaRTT.Load())
	cur := int(a.limit.Load())

	var next int
	if ewma <= a.targetRTT {
		next = cur + 1
		if next > a.maxLimit {
			next = a.maxLimit
		}
		if next != cur {
			a.metrics.LimitIncr.Add(1)
		}
	} else {
		ratio := float64(a.targetRTT) / float64(ewma)
		next = int(math.Round(float64(cur) * ratio))
		if next < a.minLimit {
			next = a.minLimit
		}
		if next > cur {
			next = cur // only decrease in this branch
		}
		if next != cur {
			a.metrics.LimitDecr.Add(1)
		}
	}

	if next == cur {
		return
	}

	delta := next - cur
	a.limit.Store(int64(next))
	a.metrics.CurrentLimit.Store(int64(next))
	a.sem.resize(delta)
}

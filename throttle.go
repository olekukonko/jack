package jack

import (
	"math"
	"sync/atomic"
	"time"
)

// throttleProbScale is the fixed-point scale for rejection probabilities.
// Probabilities are stored as integers in [0, throttleProbScale] representing [0.0, 1.0].
// Using basis points (10 000) gives 0.01% resolution, enough for any load-shedding decision.
const throttleProbScale = 10_000

// ThrottleMetrics tracks throttle behavior.
type ThrottleMetrics struct {
	RequestsTotal  atomic.Uint64
	Accepted       atomic.Uint64
	RejectedLocal  atomic.Uint64
	RejectedRemote atomic.Uint64
	// ThrottleProb is the current rejection probability scaled by throttleProbScale (0–10 000).
	ThrottleProb atomic.Uint64
}

// ThrottleOption configures a Throttle.
type ThrottleOption func(*Throttle)

// ThrottleWithRatio sets the overcommit ratio (default 2.0).
// A ratio of 2.0 means the throttle targets a 50% upstream acceptance rate before shedding.
func ThrottleWithRatio(ratio float64) ThrottleOption {
	return func(a *Throttle) {
		if ratio > 1 {
			a.ratio = ratio
		}
	}
}

// ThrottleWithWindow sets the observation window for accept/reject counters (default 1 minute).
// Counters reset after 1 000 samples regardless of window to adapt to changing load.
func ThrottleWithWindow(d time.Duration) ThrottleOption {
	return func(a *Throttle) {
		if d > 0 {
			a.window = d
		}
	}
}

// Throttle is a client-side self-tuning throttle.
// It observes upstream acceptance and rejection rates and probabilistically
// rejects local requests before sending when the upstream is overloaded.
// Lower-priority traffic is throttled more aggressively than critical traffic.
type Throttle struct {
	priorities    int
	acceptWindows []atomic.Uint64
	rejectWindows []atomic.Uint64
	// probabilities stores rejection probability as a fixed-point integer scaled by
	// throttleProbScale. Using atomic.Int64 allows lock-free reads in the hot path.
	probabilities []atomic.Int64

	ratio  float64
	window time.Duration

	randState atomic.Uint64
	metrics   *ThrottleMetrics
	closed    atomic.Bool
}

// NewThrottle creates a throttle with the given number of priorities.
// The standard jack priority count (4) is recommended.
func NewThrottle(priorities int, opts ...ThrottleOption) *Throttle {
	if priorities <= 0 {
		priorities = priorityCount
	}
	a := &Throttle{
		priorities:    priorities,
		acceptWindows: make([]atomic.Uint64, priorities),
		rejectWindows: make([]atomic.Uint64, priorities),
		probabilities: make([]atomic.Int64, priorities),
		ratio:         2.0,
		window:        time.Minute,
		metrics:       &ThrottleMetrics{},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Metrics returns the throttle's operational metrics.
func (a *Throttle) Metrics() *ThrottleMetrics {
	return a.metrics
}

// Allow returns true if the request should proceed.
// Uses a lock-free LCG random check against the per-priority rejection probability.
func (a *Throttle) Allow(p Priority) bool {
	if a.closed.Load() {
		return false
	}
	if int(p) < 0 || int(p) >= a.priorities {
		p = PriorityLow
	}

	prob := a.probabilities[int(p)].Load()
	if prob <= 0 {
		a.metrics.RequestsTotal.Add(1)
		a.metrics.Accepted.Add(1)
		return true
	}

	// Fast LCG random number generator; no allocation, safe for concurrent use.
	v := a.randState.Add(747796405) * 2891336453
	// Scale prob into the uint32 range for comparison without floating-point division.
	threshold := uint64(prob) * math.MaxUint32 / throttleProbScale
	if uint64(uint32(v>>32)) < threshold {
		a.metrics.RequestsTotal.Add(1)
		a.metrics.RejectedLocal.Add(1)
		return false
	}

	a.metrics.RequestsTotal.Add(1)
	a.metrics.Accepted.Add(1)
	return true
}

// Accepted records a successful upstream response for the given priority,
// decreasing throttle pressure for that tier.
func (a *Throttle) Accepted(p Priority) {
	if a.closed.Load() {
		return
	}
	if int(p) < 0 || int(p) >= a.priorities {
		p = PriorityLow
	}
	idx := int(p)
	a.acceptWindows[idx].Add(1)
	a.metrics.Accepted.Add(1)
	a.adjust(idx)
}

// Rejected records an upstream rejection or timeout for the given priority,
// increasing throttle pressure for that tier.
func (a *Throttle) Rejected(p Priority) {
	if a.closed.Load() {
		return
	}
	if int(p) < 0 || int(p) >= a.priorities {
		p = PriorityLow
	}
	idx := int(p)
	a.rejectWindows[idx].Add(1)
	a.metrics.RejectedRemote.Add(1)
	a.adjust(idx)
}

// adjust recomputes the rejection probability for priority index idx.
// The base probability is derived from the accept/reject ratio relative to the
// configured overcommit target. Lower priorities (higher idx) receive a larger
// multiplier so they are shed first, protecting critical traffic.
// Under full saturation (base prob == 1.0) priorities are separated linearly
// across [1/priorities … 1.0] so ordering is always strict.
func (a *Throttle) adjust(idx int) {
	accepted := float64(a.acceptWindows[idx].Load())
	rejected := float64(a.rejectWindows[idx].Load())
	total := accepted + rejected
	if total < 10 {
		return
	}

	target := 1.0 / a.ratio
	rate := accepted / total

	var baseProb float64
	if rate >= target {
		baseProb = 0
	} else {
		baseProb = (target - rate) / target
	}

	// Priority multiplier: Critical=1×, High=2×, Medium=4×, Low=8×.
	// Multiply base probability so lower-priority tiers are shed sooner.
	multiplier := float64(int(1) << idx)
	prob := baseProb * multiplier
	if prob > 1 {
		prob = 1
	}

	// When the upstream is fully saturated (baseProb == 1.0), the multiplier
	// always produces prob == 1.0 for every tier, giving no ordering separation.
	// Instead assign probabilities linearly across the priority range so that
	// Critical is always throttled less than High, High less than Medium, etc.
	if baseProb >= 1.0 {
		prob = float64(idx+1) / float64(a.priorities)
	}

	scaled := int64(prob * throttleProbScale)
	a.probabilities[idx].Store(scaled)
	a.metrics.ThrottleProb.Store(uint64(scaled))

	// Reset windows once they reach sufficient sample size so the throttle
	// adapts to changing upstream conditions rather than anchoring to old data.
	if total > 1000 {
		a.acceptWindows[idx].Store(0)
		a.rejectWindows[idx].Store(0)
	}
}

// Close shuts down the throttle; all subsequent Allow calls return false.
func (a *Throttle) Close() {
	a.closed.Store(true)
}

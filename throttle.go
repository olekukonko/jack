package jack

import (
	"math"
	"sync/atomic"
	"time"
)

// throttleProbScale is the fixed-point scale for rejection probabilities.
// Probabilities are stored as integers in [0, throttleProbScale] representing [0.0, 1.0].
// Basis points (10 000) give 0.01% resolution — sufficient for any load-shedding decision.
const throttleProbScale = 10_000

// ThrottleMetrics tracks throttle behaviour across all priority tiers.
type ThrottleMetrics struct {
	RequestsTotal  atomic.Uint64
	Accepted       atomic.Uint64
	RejectedLocal  atomic.Uint64
	RejectedRemote atomic.Uint64
	// ThrottleProbs holds the current rejection probability for each priority tier,
	// scaled by throttleProbScale (0 = no throttling, 10 000 = always throttled).
	// Index matches Priority: 0=Critical, 1=High, 2=Medium, 3=Low.
	ThrottleProbs [priorityCount]atomic.Int64
}

// ThrottleOption configures a Throttle.
type ThrottleOption func(*Throttle)

// ThrottleWithRatio sets the overcommit ratio (default 2.0).
// A ratio of 2.0 targets a 50% upstream acceptance rate before local shedding begins.
func ThrottleWithRatio(ratio float64) ThrottleOption {
	return func(a *Throttle) {
		if ratio > 1 {
			a.ratio = ratio
		}
	}
}

// ThrottleWithWindow sets the observation window for accept/reject counters (default 1 minute).
// Counters reset after windowResetSamples samples to adapt to changing upstream load.
func ThrottleWithWindow(d time.Duration) ThrottleOption {
	return func(a *Throttle) {
		if d > 0 {
			a.window = d
		}
	}
}

// ThrottleWithWindowResetSamples sets how many samples to collect before resetting the
// observation window (default 1000). Smaller values adapt faster; larger values are smoother.
func ThrottleWithWindowResetSamples(n uint64) ThrottleOption {
	return func(a *Throttle) {
		if n > 0 {
			a.windowResetSamples = n
		}
	}
}

// Throttle is a client-side self-tuning throttle.
// It observes upstream acceptance and rejection rates and probabilistically
// rejects local requests before sending when the upstream is overloaded.
// Lower-priority traffic is throttled more aggressively than critical traffic.
type Throttle struct {
	priorities         int
	acceptWindows      []atomic.Uint64
	rejectWindows      []atomic.Uint64
	probabilities      []atomic.Int64 // fixed-point, scaled by throttleProbScale
	ratio              float64
	window             time.Duration
	windowResetSamples uint64

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
		priorities:         priorities,
		acceptWindows:      make([]atomic.Uint64, priorities),
		rejectWindows:      make([]atomic.Uint64, priorities),
		probabilities:      make([]atomic.Int64, priorities),
		ratio:              2.0,
		window:             time.Minute,
		windowResetSamples: 1000,
		metrics:            &ThrottleMetrics{},
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

// Probability returns the current rejection probability for the given priority as a
// value in [0.0, 1.0]. This is the live per-priority value, not a single aggregate.
func (a *Throttle) Probability(p Priority) float64 {
	if int(p) < 0 || int(p) >= a.priorities {
		p = PriorityLow
	}
	return float64(a.probabilities[int(p)].Load()) / throttleProbScale
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

	// Fast LCG; no allocation, safe for concurrent use via atomic add.
	v := a.randState.Add(747796405) * 2891336453
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

// Accepted records a successful upstream response for the given priority tier,
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

// Rejected records an upstream rejection or timeout for the given priority tier,
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
// Base probability comes from the accept/reject ratio vs the configured target.
// Lower priorities (higher idx) use an exponential multiplier so they are shed first.
// Under full saturation (baseProb==1.0) probabilities are assigned linearly across
// [1/priorities … 1.0] to guarantee strict ordering between all tiers.
// The observation window resets after windowResetSamples to track changing load.
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
	multiplier := float64(int(1) << idx)
	prob := baseProb * multiplier
	if prob > 1 {
		prob = 1
	}

	// Under full saturation the multiplier collapses all tiers to 1.0.
	// Use linear spacing so ordering is preserved even at 100% rejection rate.
	if baseProb >= 1.0 {
		prob = float64(idx+1) / float64(a.priorities)
	}

	scaled := int64(prob * throttleProbScale)
	a.probabilities[idx].Store(scaled)
	if idx < priorityCount {
		a.metrics.ThrottleProbs[idx].Store(scaled)
	}

	// Reset window once the configured sample count is reached so the throttle
	// adapts to changing upstream conditions rather than anchoring to stale data.
	if total >= float64(a.windowResetSamples) {
		a.acceptWindows[idx].Store(0)
		a.rejectWindows[idx].Store(0)
	}
}

// Close shuts down the throttle; all subsequent Allow calls return false.
func (a *Throttle) Close() {
	a.closed.Store(true)
}

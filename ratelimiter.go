package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimiterMetrics tracks operational statistics for the rate limiter.
type RateLimiterMetrics struct {
	AllowedFast    atomic.Uint64
	AllowedSlow    atomic.Uint64
	Rejected       atomic.Uint64
	TokensConsumed atomic.Uint64
	TokensRefilled atomic.Uint64
	QueueDepth     atomic.Int64
	MaxQueueDepth  atomic.Int64
}

// RateLimiterOption configures a RateLimiter.
type RateLimiterOption func(*RateLimiter)

// RateLimiterWithMaxWait sets the hard ceiling on waiter age during CoDel dropping mode.
// In normal mode waiters are not evicted by age; their own context handles timeout.
func RateLimiterWithMaxWait(d time.Duration) RateLimiterOption {
	return func(r *RateLimiter) { r.maxWait = d }
}

// RateLimiter bounds throughput with a prioritized token bucket.
// Allow / AllowN are lock-free. Blocking Acquire uses CoDel-style priority queues.
// Replenish manually returns tokens; it is distinct from time-based refill and is
// intended for reservation patterns where the caller holds a token between operations.
type RateLimiter struct {
	ratePerSec float64
	burst      int64
	tokens     atomic.Int64 // whole tokens
	lastRefill atomic.Int64 // unix nano

	refillMu sync.Mutex

	queues  [priorityCount]waiterQueue
	queueMu sync.Mutex
	maxWait time.Duration

	metrics        *RateLimiterMetrics
	reserveMetrics *ReservationMetrics
	closed         atomic.Bool
}

// NewRateLimiter creates a token bucket rate limiter.
// ratePerSec is the sustained fill rate; burst is the maximum token capacity.
// Default maxWait (CoDel dropping threshold) is 500ms.
func NewRateLimiter(ratePerSec float64, burst int, opts ...RateLimiterOption) *RateLimiter {
	if ratePerSec < 0 {
		ratePerSec = 0
	}
	if burst <= 0 {
		burst = 1
	}
	r := &RateLimiter{
		ratePerSec:     ratePerSec,
		burst:          int64(burst),
		maxWait:        500 * time.Millisecond,
		metrics:        &RateLimiterMetrics{},
		reserveMetrics: &ReservationMetrics{},
	}
	for _, opt := range opts {
		opt(r)
	}
	r.tokens.Store(int64(burst))
	r.lastRefill.Store(time.Now().UnixNano())
	return r
}

// Metrics returns the rate limiter's operational metrics.
func (r *RateLimiter) Metrics() *RateLimiterMetrics {
	return r.metrics
}

// Tokens returns the number of tokens currently available.
func (r *RateLimiter) Tokens() int64 {
	r.refill()
	v := r.tokens.Load()
	if v < 0 {
		return 0
	}
	return v
}

// Burst returns the maximum token capacity the rate limiter was created with.
func (r *RateLimiter) Burst() int64 {
	return r.burst
}

// refill lazily adds tokens based on elapsed time since the last refill.
// Uses a CAS loop on lastRefill so only one goroutine advances the bucket per interval.
func (r *RateLimiter) refill() {
	now := time.Now().UnixNano()
	for {
		last := r.lastRefill.Load()
		delta := now - last
		if delta <= 0 {
			return
		}
		added := int64(float64(delta) / 1e9 * r.ratePerSec)
		if added <= 0 {
			return
		}
		advance := int64(float64(added) / r.ratePerSec * 1e9)
		if !r.lastRefill.CompareAndSwap(last, last+advance) {
			continue
		}
		for {
			curr := r.tokens.Load()
			newTokens := curr + added
			if newTokens > r.burst {
				newTokens = r.burst
			}
			if r.tokens.CompareAndSwap(curr, newTokens) {
				r.metrics.TokensRefilled.Add(uint64(added))
				return
			}
		}
	}
}

// Allow consumes one token if available. Non-blocking, lock-free.
func (r *RateLimiter) Allow(p Priority) bool {
	if r.closed.Load() {
		return false
	}
	r.refill()
	for {
		curr := r.tokens.Load()
		if curr <= 0 {
			return false
		}
		if r.tokens.CompareAndSwap(curr, curr-1) {
			r.metrics.AllowedFast.Add(1)
			r.metrics.TokensConsumed.Add(1)
			return true
		}
	}
}

// AllowN attempts to consume n tokens atomically. Non-blocking, lock-free.
func (r *RateLimiter) AllowN(p Priority, n int64) bool {
	if r.closed.Load() || n <= 0 {
		return false
	}
	r.refill()
	for {
		curr := r.tokens.Load()
		if curr < n {
			return false
		}
		if r.tokens.CompareAndSwap(curr, curr-n) {
			r.metrics.AllowedFast.Add(1)
			r.metrics.TokensConsumed.Add(uint64(n))
			return true
		}
	}
}

// Acquire waits for a single token, respecting priority and context cancellation.
// Higher-priority callers are dequeued before lower-priority ones already waiting.
func (r *RateLimiter) Acquire(ctx context.Context, p Priority) error {
	if int(p) < 0 || int(p) >= priorityCount {
		p = PriorityLow
	}
	if r.Allow(p) {
		return nil
	}
	if r.closed.Load() {
		return ErrRateLimiterClosed
	}

	w := &waiter{ch: make(chan struct{})}
	w.enqueueAt = time.Now().UnixNano()

	r.queueMu.Lock()
	if r.closed.Load() {
		r.queueMu.Unlock()
		return ErrRateLimiterClosed
	}
	r.queues[int(p)].push(w)
	depth := r.queueDepthLocked()
	r.queueMu.Unlock()

	r.updateQueueDepth(int64(depth))

	select {
	case <-w.ch:
		if w.err != nil {
			return w.err
		}
		return nil
	case <-ctx.Done():
		w.cancelled.Store(true)
		r.metrics.Rejected.Add(1)
		r.metrics.QueueDepth.Add(-1)
		return ctx.Err()
	}
}

// Replenish adds n tokens back to the bucket, up to the burst ceiling.
// Use this when a caller held a reservation and is returning it — for example,
// when a request is retried locally and the upstream slot does not need to be
// consumed. This is NOT the same as time-based refill.
func (r *RateLimiter) Replenish(n int64) {
	if n <= 0 {
		return
	}
	for {
		curr := r.tokens.Load()
		newTokens := curr + n
		if newTokens > r.burst {
			newTokens = r.burst
		}
		if r.tokens.CompareAndSwap(curr, newTokens) {
			break
		}
	}
	r.drainQueue()
}

// Release replenishes one token and wakes the highest-priority waiter if any are queued.
// Retained for compatibility with patterns that pair Acquire with Release. Prefer
// Replenish(1) for new code where the intent is explicit.
func (r *RateLimiter) Release() {
	r.Replenish(1)
}

// Reserve attempts to reserve n tokens from the rate limiter without blocking.
// It returns a Reservation whose Delay() tells the caller how long to wait.
// The caller can Cancel() the reservation if the delay is unacceptable.
//
// Unlike Acquire, Reserve never blocks. It is safe for concurrent use.
func (r *RateLimiter) Reserve(n int64, opts ...ReserveOption) *Reservation {
	cfg := &reserveConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	r.refill()
	now := time.Now()

	// Try fast path: tokens immediately available.
	for {
		curr := r.tokens.Load()
		if curr >= n {
			if r.tokens.CompareAndSwap(curr, curr-n) {
				res := &Reservation{
					limiter: r,
					tokens:  n,
					readyAt: now,
					metrics: r.reserveMetrics,
				}
				r.reserveMetrics.Reserved.Add(1)
				return res
			}
			continue
		}
		break
	}

	// Slow path: compute when tokens will be available via time-based refill.
	var readyAt time.Time
	if r.ratePerSec <= 0 {
		// No refill configured — tokens will never arrive via time.
		// Return a reservation with a zero readyAt so Delay()==0, but the
		// bucket is empty. The caller can still Cancel() to be explicit.
		readyAt = now
	} else {
		deficit := n - r.tokens.Load()
		if deficit < 0 {
			deficit = 0
		}
		waitDuration := time.Duration(float64(deficit) / r.ratePerSec * float64(time.Second))
		readyAt = now.Add(waitDuration)
	}

	res := &Reservation{
		limiter: r,
		tokens:  n,
		readyAt: readyAt,
		metrics: r.reserveMetrics,
	}

	if cfg.maxDelay > 0 && time.Until(readyAt) > cfg.maxDelay {
		res.cancelled.Store(true)
		r.reserveMetrics.Dropped.Add(1)
		return res
	}

	// Consume the tokens speculatively — they will be available by readyAt.
	// If the caller Cancels before readyAt, Replenish returns them.
	for {
		curr := r.tokens.Load()
		next := curr - n
		if r.tokens.CompareAndSwap(curr, next) {
			break
		}
	}

	r.reserveMetrics.Reserved.Add(1)
	return res
}

// ReserveMetrics returns the metrics for the reservation subsystem.
// Returns nil if Reserve has never been called.
func (r *RateLimiter) ReserveMetrics() *ReservationMetrics {
	return r.reserveMetrics
}

// drainQueue wakes the highest-priority waiter if a token is available.
func (r *RateLimiter) drainQueue() {
	for {
		r.queueMu.Lock()
		w := r.findWaiterLocked()
		r.queueMu.Unlock()

		if w == nil {
			return
		}
		if w.cancelled.Load() {
			continue
		}

		for {
			curr := r.tokens.Load()
			if curr <= 0 {
				return
			}
			if r.tokens.CompareAndSwap(curr, curr-1) {
				w.closeCh()
				r.metrics.AllowedSlow.Add(1)
				r.metrics.TokensConsumed.Add(1)
				r.metrics.QueueDepth.Add(-1)
				return
			}
		}
	}
}

// Close shuts down the rate limiter, unblocking all pending waiters with ErrRateLimiterClosed.
func (r *RateLimiter) Close() {
	r.closed.Store(true)
	r.queueMu.Lock()
	for p := 0; p < priorityCount; p++ {
		for {
			w := r.queues[p].popFIFO()
			if w == nil {
				break
			}
			w.cancelled.Store(true)
			w.closeChWithErr(ErrRateLimiterClosed)
		}
	}
	r.queueMu.Unlock()
	r.metrics.QueueDepth.Store(0)
}

// queueDepthLocked sums all per-priority queue lengths; caller holds queueMu.
func (r *RateLimiter) queueDepthLocked() int {
	n := 0
	for p := 0; p < priorityCount; p++ {
		n += r.queues[p].len()
	}
	return n
}

// updateQueueDepth stores the depth and updates the high-water mark.
func (r *RateLimiter) updateQueueDepth(depth int64) {
	r.metrics.QueueDepth.Store(depth)
	for {
		current := r.metrics.MaxQueueDepth.Load()
		if depth <= current {
			break
		}
		if r.metrics.MaxQueueDepth.CompareAndSwap(current, depth) {
			break
		}
	}
}

// findWaiterLocked selects the next waiter using priority-first, CoDel-aware policy.
// In dropping mode, waiters older than maxWait are evicted via closeChWithErr.
// In normal mode, age eviction is skipped — context deadline handles waiter timeout.
// Caller must hold queueMu.
func (r *RateLimiter) findWaiterLocked() *waiter {
	now := time.Now().UnixNano()

	oldest := int64(0)
	for p := 0; p < priorityCount; p++ {
		if w := r.queues[p].peek(); w != nil {
			if oldest == 0 || w.enqueueAt < oldest {
				oldest = w.enqueueAt
			}
		}
	}

	dropping := oldest > 0 && time.Duration(now-oldest) > r.maxWait/2

	for p := 0; p < priorityCount; p++ {
		q := &r.queues[p]
		if q.len() == 0 {
			continue
		}

		pop := q.popFIFO
		if dropping {
			pop = q.popLIFO
		}

		for q.len() > 0 {
			w := pop()
			if w == nil {
				break
			}
			if w.cancelled.Load() {
				r.metrics.QueueDepth.Add(-1)
				continue
			}
			if dropping && time.Duration(now-w.enqueueAt) > r.maxWait {
				w.cancelled.Store(true)
				w.closeChWithErr(context.DeadlineExceeded)
				r.metrics.QueueDepth.Add(-1)
				continue
			}
			return w
		}
	}
	return nil
}

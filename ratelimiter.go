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
// Allow is lock-free. Blocking Acquire uses CoDel-style priority queues.
type RateLimiter struct {
	ratePerSec float64
	burst      int64
	tokens     atomic.Int64 // whole tokens
	lastRefill atomic.Int64 // unix nano

	refillMu sync.Mutex // only for lazy refill calculation

	queues  [priorityCount]waiterQueue
	queueMu sync.Mutex
	maxWait time.Duration

	metrics *RateLimiterMetrics
	closed  atomic.Bool
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
		ratePerSec: ratePerSec,
		burst:      int64(burst),
		maxWait:    500 * time.Millisecond,
		metrics:    &RateLimiterMetrics{},
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

// AllowN attempts to consume n tokens. Non-blocking, lock-free.
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

	r.metrics.QueueDepth.Store(int64(depth))
	for {
		current := r.metrics.MaxQueueDepth.Load()
		if int64(depth) <= current {
			break
		}
		if r.metrics.MaxQueueDepth.CompareAndSwap(current, int64(depth)) {
			break
		}
	}

	select {
	case <-w.ch:
		// w.err is set by closeChWithErr when the waiter is rejected (Close or CoDel drop).
		// A nil err means a token was granted normally.
		if w.err != nil {
			return w.err
		}
		return nil
	case <-ctx.Done():
		w.cancelled.Store(true)
		r.metrics.Rejected.Add(1)
		return ctx.Err()
	}
}

// Release returns one token and attempts to wake the highest-priority waiter.
func (r *RateLimiter) Release() {
	for {
		curr := r.tokens.Load()
		newTokens := curr + 1
		if newTokens > r.burst {
			newTokens = r.burst
		}
		if r.tokens.CompareAndSwap(curr, newTokens) {
			break
		}
	}

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
}

func (r *RateLimiter) queueDepthLocked() int {
	n := 0
	for p := 0; p < priorityCount; p++ {
		n += r.queues[p].len()
	}
	return n
}

// findWaiterLocked selects the next waiter using priority-first, CoDel-aware policy.
// In dropping mode, waiters older than maxWait are evicted via closeChWithErr so they
// wake immediately with an error. In normal mode, age-based eviction is skipped —
// the waiter's own context handles timeout, preserving priority ordering correctness.
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

	dropping := false
	if oldest > 0 && time.Duration(now-oldest) > r.maxWait/2 {
		dropping = true
	}

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
				continue
			}
			// In dropping mode, actively evict stale waiters and wake them with an error.
			// In normal mode, skip this check — context deadline handles waiter timeout.
			if dropping && time.Duration(now-w.enqueueAt) > r.maxWait {
				w.cancelled.Store(true)
				w.closeChWithErr(context.DeadlineExceeded)
				continue
			}
			return w
		}
	}
	return nil
}

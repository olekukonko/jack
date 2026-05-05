package jack

import (
	"errors"
	"sync/atomic"
)

// Priority defines request importance for backpressure decisions.
// Lower numeric values indicate higher priority (Critical=0 is served first).
type Priority int

const (
	PriorityCritical Priority = iota // Admin, user-facing critical paths
	PriorityHigh                     // Standard user requests
	PriorityMedium                   // Async work, type-ahead
	PriorityLow                      // Backfill, batch, probes
)

const priorityCount = 4

// waiterQueueCompactThreshold is the number of pops after which the backing
// slice is compacted to reclaim memory consumed by FIFO head-slicing.
const waiterQueueCompactThreshold = 64

var (
	ErrSemaphoreClosed   = errors.New("semaphore closed")
	ErrRateLimiterClosed = errors.New("rate limiter closed")
	ErrThrottleClosed    = errors.New("adaptive throttle closed")
	ErrQueueClosed       = errors.New("priority queue closed")
)

// backpressureMetrics is embedded by all backpressure types.
type backpressureMetrics struct {
	RequestsTotal atomic.Uint64
	Accepted      atomic.Uint64
	Rejected      atomic.Uint64
}

// waiter is a shared blocking primitive used by Semaphore and RateLimiter.
// err is set before closing ch so the waiting goroutine can distinguish
// a successful grant (err==nil) from a close or CoDel drop (err!=nil).
type waiter struct {
	ch        chan struct{}
	enqueueAt int64
	cancelled atomic.Bool
	closed    atomic.Bool
	err       error // non-nil when closed due to Close() or CoDel drop
}

// closeCh closes the waiter channel exactly once.
func (w *waiter) closeCh() {
	if w.closed.CompareAndSwap(false, true) {
		close(w.ch)
	}
}

// closeChWithErr records the reason then closes the channel exactly once.
// The waiting goroutine reads w.err after waking to return the right error.
func (w *waiter) closeChWithErr(err error) {
	w.err = err
	w.closeCh()
}

// waiterQueue holds blocked goroutines for a single priority level.
// It supports both FIFO (fairness) and LIFO (overload) extraction.
// The head index avoids repeated slice copies on FIFO pops; the slice
// is compacted every waiterQueueCompactThreshold pops to bound memory.
type waiterQueue struct {
	waiters  []*waiter
	head     int
	popCount int
}

// push appends a waiter to the end of the queue.
func (q *waiterQueue) push(w *waiter) {
	q.waiters = append(q.waiters, w)
}

// popFIFO removes and returns the oldest waiter; compacts the slice periodically.
func (q *waiterQueue) popFIFO() *waiter {
	if q.head >= len(q.waiters) {
		return nil
	}
	w := q.waiters[q.head]
	q.waiters[q.head] = nil // release reference
	q.head++
	q.popCount++
	if q.popCount >= waiterQueueCompactThreshold {
		q.waiters = append(q.waiters[:0], q.waiters[q.head:]...)
		q.head = 0
		q.popCount = 0
	}
	return w
}

// popLIFO removes and returns the newest waiter (used under CoDel dropping mode).
func (q *waiterQueue) popLIFO() *waiter {
	n := len(q.waiters)
	if q.head >= n {
		return nil
	}
	last := n - 1
	w := q.waiters[last]
	q.waiters[last] = nil
	q.waiters = q.waiters[:last]
	return w
}

// peek returns the oldest waiter without removing it.
func (q *waiterQueue) peek() *waiter {
	if q.head >= len(q.waiters) {
		return nil
	}
	return q.waiters[q.head]
}

// len returns the number of waiters currently in the queue.
func (q *waiterQueue) len() int {
	return len(q.waiters) - q.head
}

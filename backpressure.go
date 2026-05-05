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

var (
	ErrSemaphoreClosed   = errors.New("semaphore closed")
	ErrRateLimiterClosed = errors.New("rate limiter closed")
	ErrThrottleClosed    = errors.New("adaptive throttle closed")
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
type waiterQueue struct {
	waiters []*waiter
}

func (q *waiterQueue) push(w *waiter) {
	q.waiters = append(q.waiters, w)
}

func (q *waiterQueue) popFIFO() *waiter {
	if len(q.waiters) == 0 {
		return nil
	}
	w := q.waiters[0]
	q.waiters = q.waiters[1:]
	return w
}

func (q *waiterQueue) popLIFO() *waiter {
	n := len(q.waiters)
	if n == 0 {
		return nil
	}
	w := q.waiters[n-1]
	q.waiters = q.waiters[:n-1]
	return w
}

func (q *waiterQueue) peek() *waiter {
	if len(q.waiters) == 0 {
		return nil
	}
	return q.waiters[0]
}

func (q *waiterQueue) len() int {
	return len(q.waiters)
}

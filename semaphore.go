package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// SemaphoreMetrics tracks operational statistics for the semaphore.
// All fields are safe for concurrent reads without additional locking.
type SemaphoreMetrics struct {
	AcquiredFast  atomic.Uint64
	AcquiredSlow  atomic.Uint64
	Released      atomic.Uint64
	Rejected      atomic.Uint64
	Timeouts      atomic.Uint64
	QueueDepth    atomic.Int64
	MaxQueueDepth atomic.Int64
}

// SemaphoreOption configures a Semaphore.
type SemaphoreOption func(*Semaphore)

// SemaphoreWithTargetSojourn sets the CoDel target sojourn time.
// When the oldest waiter exceeds this threshold, the queue enters dropping mode.
func SemaphoreWithTargetSojourn(d time.Duration) SemaphoreOption {
	return func(s *Semaphore) { s.targetSojourn = d }
}

// SemaphoreWithMaxSojourn sets the hard ceiling on waiter age in CoDel dropping mode.
// Waiters older than this are discarded only while the queue is actively shedding load.
func SemaphoreWithMaxSojourn(d time.Duration) SemaphoreOption {
	return func(s *Semaphore) { s.maxSojourn = d }
}

// SemaphoreWithInterval sets the CoDel interval for sustained overload detection.
// Dropping mode engages when sojourn stays above target for this long.
func SemaphoreWithInterval(d time.Duration) SemaphoreOption {
	return func(s *Semaphore) { s.interval = d }
}

// Semaphore bounds concurrent access with priority and CoDel queueing.
// The fast path (TryAcquire) is lock-free. Blocking acquire uses per-priority
// queues that switch from FIFO to LIFO under sustained overload.
type Semaphore struct {
	capacity      int64
	available     atomic.Int64
	queues        [priorityCount]waiterQueue
	queueMu       sync.Mutex
	targetSojourn time.Duration
	maxSojourn    time.Duration
	interval      time.Duration
	dropping      atomic.Bool
	firstAbove    atomic.Int64 // unix nano; 0 means unset
	metrics       *SemaphoreMetrics
	closed        atomic.Bool
}

// NewSemaphore creates a prioritized semaphore with the given capacity.
// Defaults: target sojourn 5ms, max sojourn 500ms, interval 100ms.
func NewSemaphore(capacity int, opts ...SemaphoreOption) *Semaphore {
	if capacity <= 0 {
		capacity = 1
	}
	s := &Semaphore{
		capacity:      int64(capacity),
		targetSojourn: 5 * time.Millisecond,
		maxSojourn:    500 * time.Millisecond,
		interval:      100 * time.Millisecond,
		metrics:       &SemaphoreMetrics{},
	}
	for _, opt := range opts {
		opt(s)
	}
	s.available.Store(int64(capacity))
	return s
}

// Metrics returns the semaphore's operational metrics.
func (s *Semaphore) Metrics() *SemaphoreMetrics {
	return s.metrics
}

// TryAcquire attempts to take a slot without blocking.
// Returns false immediately if no slot is available or the semaphore is closed.
func (s *Semaphore) TryAcquire(p Priority) bool {
	if s.closed.Load() {
		return false
	}
	for {
		avail := s.available.Load()
		if avail <= 0 {
			return false
		}
		if s.available.CompareAndSwap(avail, avail-1) {
			s.metrics.AcquiredFast.Add(1)
			return true
		}
	}
}

// Acquire waits for a slot, respecting priority and context cancellation.
// Higher-priority callers are served before lower-priority ones already queued.
func (s *Semaphore) Acquire(ctx context.Context, p Priority) error {
	if int(p) < 0 || int(p) >= priorityCount {
		p = PriorityLow
	}
	if s.TryAcquire(p) {
		return nil
	}
	if s.closed.Load() {
		return ErrSemaphoreClosed
	}

	w := &waiter{ch: make(chan struct{})}
	w.enqueueAt = time.Now().UnixNano()

	s.queueMu.Lock()
	if s.closed.Load() {
		s.queueMu.Unlock()
		return ErrSemaphoreClosed
	}
	s.queues[int(p)].push(w)
	depth := s.queueDepthLocked()
	s.queueMu.Unlock()

	s.metrics.QueueDepth.Store(int64(depth))
	for {
		current := s.metrics.MaxQueueDepth.Load()
		if int64(depth) <= current {
			break
		}
		if s.metrics.MaxQueueDepth.CompareAndSwap(current, int64(depth)) {
			break
		}
	}

	select {
	case <-w.ch:
		// w.err is set by closeChWithErr when the waiter is rejected (Close or CoDel drop).
		// A nil err means the slot was granted normally.
		if w.err != nil {
			return w.err
		}
		return nil
	case <-ctx.Done():
		w.cancelled.Store(true)
		s.metrics.Rejected.Add(1)
		return ctx.Err()
	}
}

// Release returns a slot and attempts to hand it to the highest-priority waiter.
// If the dequeued waiter was context-cancelled, Release retries until it finds a live one.
func (s *Semaphore) Release() {
	s.available.Add(1)
	s.metrics.Released.Add(1)

	for {
		s.queueMu.Lock()
		w := s.findWaiterLocked()
		s.queueMu.Unlock()

		if w == nil {
			return
		}
		if w.cancelled.Load() {
			continue
		}

		for {
			avail := s.available.Load()
			if avail <= 0 {
				return
			}
			if s.available.CompareAndSwap(avail, avail-1) {
				w.closeCh()
				s.metrics.AcquiredSlow.Add(1)
				return
			}
		}
	}
}

// Close permanently shuts down the semaphore, unblocking all pending waiters with ErrSemaphoreClosed.
func (s *Semaphore) Close() {
	s.closed.Store(true)
	s.queueMu.Lock()
	for p := 0; p < priorityCount; p++ {
		for {
			w := s.queues[p].popFIFO()
			if w == nil {
				break
			}
			w.cancelled.Store(true)
			w.closeChWithErr(ErrSemaphoreClosed)
		}
	}
	s.queueMu.Unlock()
}

// queueDepthLocked returns the total number of queued waiters across all priorities.
// Caller must hold queueMu.
func (s *Semaphore) queueDepthLocked() int {
	n := 0
	for p := 0; p < priorityCount; p++ {
		n += s.queues[p].len()
	}
	return n
}

// findWaiterLocked selects the next waiter using priority-first, CoDel-aware policy.
// In dropping mode, it evicts stale waiters (age > maxSojourn) via closeChWithErr
// so they wake immediately with an error rather than blocking until context expiry.
// In normal mode, stale eviction is skipped — the waiter's own context handles timeout.
// Caller must hold queueMu.
func (s *Semaphore) findWaiterLocked() *waiter {
	now := time.Now().UnixNano()

	// CoDel overload detection: measure oldest waiter age across all priorities.
	oldest := int64(0)
	for p := 0; p < priorityCount; p++ {
		if w := s.queues[p].peek(); w != nil {
			if oldest == 0 || w.enqueueAt < oldest {
				oldest = w.enqueueAt
			}
		}
	}

	if oldest > 0 {
		sojourn := time.Duration(now - oldest)
		if sojourn > s.maxSojourn {
			// Hard ceiling exceeded: engage dropping immediately regardless of interval.
			s.dropping.Store(true)
		} else if sojourn > s.targetSojourn {
			fa := s.firstAbove.Load()
			if fa == 0 {
				s.firstAbove.Store(now)
			} else if time.Duration(now-fa) > s.interval {
				s.dropping.Store(true)
			}
		} else if sojourn < s.targetSojourn/2 {
			s.dropping.Store(false)
			s.firstAbove.Store(0)
		}
	} else {
		s.dropping.Store(false)
		s.firstAbove.Store(0)
	}

	dropping := s.dropping.Load()

	// Serve highest priority first. Within a priority level, use LIFO under overload
	// (shed the newest waiters) and FIFO under normal load (fairness).
	for p := 0; p < priorityCount; p++ {
		q := &s.queues[p]
		if q.len() == 0 {
			continue
		}

		if dropping {
			for q.len() > 0 {
				w := q.popLIFO()
				if w == nil {
					break
				}
				if w.cancelled.Load() {
					continue
				}
				// In dropping mode, actively evict waiters that have aged past maxSojourn.
				// closeChWithErr wakes the goroutine immediately instead of leaving it
				// blocked until its context deadline fires.
				if time.Duration(now-w.enqueueAt) > s.maxSojourn {
					w.cancelled.Store(true)
					w.closeChWithErr(context.DeadlineExceeded)
					s.metrics.Timeouts.Add(1)
					continue
				}
				return w
			}
		} else {
			for q.len() > 0 {
				w := q.popFIFO()
				if w == nil {
					break
				}
				if w.cancelled.Load() {
					continue
				}
				// In normal mode do not evict by age — the waiter's own context handles
				// timeout. Evicting here would break priority ordering tests that use a
				// short sleep before Release.
				return w
			}
		}
	}
	return nil
}

package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// QueueMetrics tracks operational statistics for the priority queue.
type QueueMetrics struct {
	Enqueued  atomic.Uint64
	Dequeued  atomic.Uint64
	Dropped   atomic.Uint64
	Timeouts  atomic.Uint64
	Depth     atomic.Int64
	MaxDepth  atomic.Int64
	Saturated atomic.Uint64 // times the queue was full at enqueue time
}

// QueueOption configures a Queue.
type QueueOption func(*Queue)

// QueueWithCapacity sets the maximum number of items the queue can hold per priority level.
// Total capacity is capacity × priorityCount. Default is 256 per level.
func QueueWithCapacity(capacity int) QueueOption {
	return func(q *Queue) {
		if capacity > 0 {
			q.capacity = capacity
		}
	}
}

// QueueWithWorkers sets the number of concurrent consumer goroutines (default 1).
func QueueWithWorkers(n int) QueueOption {
	return func(q *Queue) {
		if n > 0 {
			q.workers = n
		}
	}
}

// QueueWithTimeout sets the maximum time an item may wait in the queue before
// being dropped. Zero means no timeout (items wait until a worker is free).
func QueueWithTimeout(d time.Duration) QueueOption {
	return func(q *Queue) { q.itemTimeout = d }
}

// queueItem wraps a user payload with enqueueing metadata.
type queueItem struct {
	payload    any
	enqueuedAt time.Time
	p          Priority
}

// Queue is a bounded, multi-priority, multi-consumer work queue.
// Higher-priority items are always dequeued before lower-priority ones.
// Under saturation it drops the lowest-priority items first (tail-drop per tier).
// This makes it suitable as the entry point of a load-balancer or transaction
// processor pipeline: pair it with a Semaphore or RateLimiter downstream.
type Queue struct {
	mu          sync.Mutex
	bins        [priorityCount][]queueItem // one bin per priority level
	capacity    int
	workers     int
	itemTimeout time.Duration
	handler     func(context.Context, any) error
	metrics     *QueueMetrics
	closed      atomic.Bool
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	notifyCh    chan struct{} // wakes workers when items are enqueued
}

// NewQueue creates a priority queue that dispatches items to handler concurrently.
// handler is called once per item with the queue's context; if it returns a non-nil
// error the item is counted as failed but the queue keeps running.
func NewQueue(handler func(context.Context, any) error, opts ...QueueOption) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		capacity: 256,
		workers:  1,
		handler:  handler,
		metrics:  &QueueMetrics{},
		ctx:      ctx,
		cancel:   cancel,
		notifyCh: make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(q)
	}
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.consume()
	}
	return q
}

// Enqueue adds an item at the given priority. It returns ErrQueueFull if the
// per-priority bin is at capacity, or ErrQueueClosed if the queue is shut down.
func (q *Queue) Enqueue(p Priority, item any) error {
	if q.closed.Load() {
		return ErrQueueClosed
	}
	if int(p) < 0 || int(p) >= priorityCount {
		p = PriorityLow
	}

	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return ErrQueueClosed
	}
	if len(q.bins[int(p)]) >= q.capacity {
		q.mu.Unlock()
		q.metrics.Saturated.Add(1)
		q.metrics.Dropped.Add(1)
		return ErrQueueFull
	}
	q.bins[int(p)] = append(q.bins[int(p)], queueItem{
		payload:    item,
		enqueuedAt: time.Now(),
		p:          p,
	})
	depth := q.totalDepthLocked()
	q.mu.Unlock()

	q.updateDepth(int64(depth))
	q.metrics.Enqueued.Add(1)
	select {
	case q.notifyCh <- struct{}{}:
	default:
	}
	return nil
}

// EnqueueCtx adds an item at the given priority, blocking until space is available,
// the context is cancelled, or the queue is closed.
func (q *Queue) EnqueueCtx(ctx context.Context, p Priority, item any) error {
	for {
		err := q.Enqueue(p, item)
		if err == nil || err == ErrQueueClosed {
			return err
		}
		// Queue full — wait for a slot to open or context to cancel.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.notifyCh:
		case <-time.After(time.Millisecond):
		}
		if q.closed.Load() {
			return ErrQueueClosed
		}
	}
}

// Depth returns the total number of items currently in the queue across all priorities.
func (q *Queue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.totalDepthLocked()
}

// DepthByPriority returns the number of items waiting at each priority level.
func (q *Queue) DepthByPriority() [priorityCount]int {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out [priorityCount]int
	for i := 0; i < priorityCount; i++ {
		out[i] = len(q.bins[i])
	}
	return out
}

// Metrics returns the queue's operational metrics.
func (q *Queue) Metrics() *QueueMetrics {
	return q.metrics
}

// Close stops accepting new items and waits for all workers to drain and exit.
func (q *Queue) Close() {
	if !q.closed.CompareAndSwap(false, true) {
		return
	}
	q.cancel()
	// Wake all sleeping workers so they can exit.
	for i := 0; i < q.workers; i++ {
		select {
		case q.notifyCh <- struct{}{}:
		default:
		}
	}
	q.wg.Wait()
}

// consume is the worker loop: it dequeues the highest-priority item and calls handler.
func (q *Queue) consume() {
	defer q.wg.Done()
	for {
		item, ok := q.dequeue()
		if !ok {
			if q.closed.Load() {
				return
			}
			select {
			case <-q.ctx.Done():
				return
			case <-q.notifyCh:
				continue
			}
		}

		// Honour per-item timeout if configured.
		if q.itemTimeout > 0 && time.Since(item.enqueuedAt) > q.itemTimeout {
			q.metrics.Timeouts.Add(1)
			q.metrics.Dropped.Add(1)
			q.metrics.Depth.Add(-1)
			continue
		}

		q.metrics.Dequeued.Add(1)
		q.metrics.Depth.Add(-1)
		// Notify after dequeue so EnqueueCtx waiters can retry.
		select {
		case q.notifyCh <- struct{}{}:
		default:
		}
		q.handler(q.ctx, item.payload) //nolint:errcheck — errors are caller's concern
	}
}

// dequeue pops the highest-priority non-empty bin. Returns false if all bins are empty.
func (q *Queue) dequeue() (queueItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for p := 0; p < priorityCount; p++ {
		if len(q.bins[p]) > 0 {
			item := q.bins[p][0]
			q.bins[p][0] = queueItem{} // release reference
			q.bins[p] = q.bins[p][1:]
			return item, true
		}
	}
	return queueItem{}, false
}

func (q *Queue) totalDepthLocked() int {
	n := 0
	for i := 0; i < priorityCount; i++ {
		n += len(q.bins[i])
	}
	return n
}

func (q *Queue) updateDepth(depth int64) {
	q.metrics.Depth.Store(depth)
	for {
		cur := q.metrics.MaxDepth.Load()
		if depth <= cur {
			break
		}
		if q.metrics.MaxDepth.CompareAndSwap(cur, depth) {
			break
		}
	}
}

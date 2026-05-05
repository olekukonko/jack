package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueBasicEnqueueDequeue(t *testing.T) {
	var received []any
	var mu sync.Mutex

	q := NewQueue(func(_ context.Context, item any) error {
		mu.Lock()
		received = append(received, item)
		mu.Unlock()
		return nil
	}, QueueWithWorkers(1))
	defer q.Close()

	for i := 0; i < 5; i++ {
		if err := q.Enqueue(PriorityHigh, i); err != nil {
			t.Fatalf("unexpected enqueue error: %v", err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(received) != 5 {
		t.Fatalf("expected 5 items processed, got %d", len(received))
	}
	mu.Unlock()
}

func TestQueuePriorityOrdering(t *testing.T) {
	ready := make(chan struct{})
	var order []Priority
	var mu sync.Mutex
	processed := make(chan struct{}, 10)

	q := NewQueue(func(_ context.Context, item any) error {
		<-ready
		mu.Lock()
		order = append(order, item.(Priority))
		mu.Unlock()
		processed <- struct{}{}
		return nil
	}, QueueWithWorkers(1), QueueWithCapacity(16))
	defer q.Close()

	// Block the worker on the first item so subsequent enqueues queue up.
	q.Enqueue(PriorityCritical, PriorityCritical) //nolint:errcheck

	time.Sleep(10 * time.Millisecond)

	q.Enqueue(PriorityLow, PriorityLow)           //nolint:errcheck
	q.Enqueue(PriorityMedium, PriorityMedium)     //nolint:errcheck
	q.Enqueue(PriorityHigh, PriorityHigh)         //nolint:errcheck
	q.Enqueue(PriorityCritical, PriorityCritical) //nolint:errcheck

	// Unblock all items.
	for i := 0; i < 5; i++ {
		ready <- struct{}{}
	}
	for i := 0; i < 5; i++ {
		<-processed
	}

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 5 {
		t.Fatalf("expected 5 processed, got %d", len(order))
	}
	// First item was already in flight; remaining should come out Critical first.
	if order[1] != PriorityCritical {
		t.Fatalf("expected second dequeue to be Critical, got %v", order[1])
	}
	if order[len(order)-1] != PriorityLow {
		t.Fatalf("expected last dequeue to be Low, got %v", order[len(order)-1])
	}
}

func TestQueueCapacityEnforced(t *testing.T) {
	block := make(chan struct{})
	q := NewQueue(func(_ context.Context, _ any) error {
		<-block
		return nil
	}, QueueWithWorkers(1), QueueWithCapacity(2))
	defer func() { close(block); q.Close() }()

	q.Enqueue(PriorityHigh, "a") //nolint:errcheck — fills worker slot
	time.Sleep(10 * time.Millisecond)

	q.Enqueue(PriorityHigh, "b") //nolint:errcheck
	q.Enqueue(PriorityHigh, "c") //nolint:errcheck

	err := q.Enqueue(PriorityHigh, "d")
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestQueueClosedRejectsEnqueue(t *testing.T) {
	q := NewQueue(func(_ context.Context, _ any) error { return nil })
	q.Close()

	if err := q.Enqueue(PriorityHigh, "x"); err != ErrQueueClosed {
		t.Fatalf("expected ErrQueueClosed after close, got %v", err)
	}
}

func TestQueueItemTimeout(t *testing.T) {
	block := make(chan struct{})
	var dropped atomic.Uint64

	q := NewQueue(func(_ context.Context, item any) error {
		<-block
		return nil
	},
		QueueWithWorkers(1),
		QueueWithCapacity(32),
		QueueWithTimeout(20*time.Millisecond),
	)
	defer func() { close(block); q.Close() }()

	q.Enqueue(PriorityHigh, "hold") //nolint:errcheck
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < 5; i++ {
		q.Enqueue(PriorityLow, i) //nolint:errcheck
	}

	time.Sleep(40 * time.Millisecond)
	block <- struct{}{}

	time.Sleep(20 * time.Millisecond)
	_ = dropped
	if q.Metrics().Timeouts.Load() == 0 {
		t.Fatal("expected some items to be timed out")
	}
}

func TestQueueMetrics(t *testing.T) {
	done := make(chan struct{})
	q := NewQueue(func(_ context.Context, _ any) error {
		<-done
		return nil
	}, QueueWithWorkers(1), QueueWithCapacity(8))
	defer func() { close(done); q.Close() }()

	for i := 0; i < 4; i++ {
		q.Enqueue(PriorityHigh, i) //nolint:errcheck
	}

	time.Sleep(10 * time.Millisecond)
	m := q.Metrics()
	if m.Enqueued.Load() != 4 {
		t.Fatalf("expected 4 enqueued, got %d", m.Enqueued.Load())
	}
}

func TestQueueDepth(t *testing.T) {
	block := make(chan struct{})
	q := NewQueue(func(_ context.Context, _ any) error {
		<-block
		return nil
	}, QueueWithWorkers(1), QueueWithCapacity(16))
	defer func() { close(block); q.Close() }()

	q.Enqueue(PriorityHigh, 1) //nolint:errcheck
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 5; i++ {
		q.Enqueue(PriorityHigh, i) //nolint:errcheck
	}

	d := q.Depth()
	if d < 1 {
		t.Fatalf("expected depth >= 1 while worker is blocked, got %d", d)
	}
}

func TestQueueDepthByPriority(t *testing.T) {
	block := make(chan struct{})
	q := NewQueue(func(_ context.Context, _ any) error {
		<-block
		return nil
	}, QueueWithWorkers(1), QueueWithCapacity(16))
	defer func() { close(block); q.Close() }()

	q.Enqueue(PriorityHigh, 1) //nolint:errcheck
	time.Sleep(10 * time.Millisecond)

	q.Enqueue(PriorityCritical, "c1") //nolint:errcheck
	q.Enqueue(PriorityCritical, "c2") //nolint:errcheck
	q.Enqueue(PriorityLow, "l1")      //nolint:errcheck

	depths := q.DepthByPriority()
	if depths[int(PriorityCritical)] != 2 {
		t.Fatalf("expected 2 critical items, got %d", depths[int(PriorityCritical)])
	}
	if depths[int(PriorityLow)] != 1 {
		t.Fatalf("expected 1 low item, got %d", depths[int(PriorityLow)])
	}
}

func TestQueueEnqueueCtxCancelled(t *testing.T) {
	block := make(chan struct{})
	q := NewQueue(func(_ context.Context, _ any) error {
		<-block
		return nil
	}, QueueWithWorkers(1), QueueWithCapacity(1))
	defer func() { close(block); q.Close() }()

	q.Enqueue(PriorityHigh, "fill") //nolint:errcheck
	time.Sleep(10 * time.Millisecond)
	q.Enqueue(PriorityHigh, "full") //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := q.EnqueueCtx(ctx, PriorityHigh, "blocked")
	if err == nil {
		t.Fatal("expected error from EnqueueCtx when queue is full and context expires")
	}
}

func TestQueueConcurrentStress(t *testing.T) {
	var processed atomic.Uint64
	q := NewQueue(func(_ context.Context, _ any) error {
		processed.Add(1)
		return nil
	}, QueueWithWorkers(8), QueueWithCapacity(512))
	defer q.Close()

	var wg sync.WaitGroup
	const producers = 16
	const itemsEach = 100

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p := Priority(id % priorityCount)
			for j := 0; j < itemsEach; j++ {
				q.Enqueue(p, j) //nolint:errcheck
			}
		}(i)
	}
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() >= producers*itemsEach {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if processed.Load() < producers*itemsEach {
		t.Fatalf("expected %d processed, got %d", producers*itemsEach, processed.Load())
	}
}

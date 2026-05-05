package jack

import (
	"testing"
	"time"
)

func TestPriorityConstants(t *testing.T) {
	if PriorityCritical != 0 {
		t.Fatal("PriorityCritical must be 0")
	}
	if PriorityHigh != 1 {
		t.Fatal("PriorityHigh must be 1")
	}
	if PriorityMedium != 2 {
		t.Fatal("PriorityMedium must be 2")
	}
	if PriorityLow != 3 {
		t.Fatal("PriorityLow must be 3")
	}
}

func TestWaiterQueueFIFO(t *testing.T) {
	q := &waiterQueue{}
	w1 := &waiter{ch: make(chan struct{}, 1)}
	w2 := &waiter{ch: make(chan struct{}, 1)}
	w3 := &waiter{ch: make(chan struct{}, 1)}

	q.push(w1)
	q.push(w2)
	q.push(w3)

	if q.len() != 3 {
		t.Fatalf("expected len 3, got %d", q.len())
	}
	if q.popFIFO() != w1 {
		t.Fatal("expected w1 first")
	}
	if q.popFIFO() != w2 {
		t.Fatal("expected w2 second")
	}
	if q.popFIFO() != w3 {
		t.Fatal("expected w3 third")
	}
	if q.popFIFO() != nil {
		t.Fatal("expected nil on empty queue")
	}
}

func TestWaiterQueueLIFO(t *testing.T) {
	q := &waiterQueue{}
	w1 := &waiter{ch: make(chan struct{}, 1)}
	w2 := &waiter{ch: make(chan struct{}, 1)}
	w3 := &waiter{ch: make(chan struct{}, 1)}

	q.push(w1)
	q.push(w2)
	q.push(w3)

	if q.popLIFO() != w3 {
		t.Fatal("expected w3 first")
	}
	if q.popLIFO() != w2 {
		t.Fatal("expected w2 second")
	}
	if q.popLIFO() != w1 {
		t.Fatal("expected w1 third")
	}
	if q.popLIFO() != nil {
		t.Fatal("expected nil on empty queue")
	}
}

func TestWaiterQueuePeek(t *testing.T) {
	q := &waiterQueue{}
	if q.peek() != nil {
		t.Fatal("expected nil peek on empty")
	}
	w := &waiter{ch: make(chan struct{}, 1)}
	q.push(w)
	if q.peek() != w {
		t.Fatal("expected w on peek")
	}
	q.popFIFO()
	if q.peek() != nil {
		t.Fatal("expected nil after pop")
	}
}

func TestWaiterCancellation(t *testing.T) {
	w := &waiter{ch: make(chan struct{}, 1), enqueueAt: time.Now().UnixNano()}
	if w.cancelled.Load() {
		t.Fatal("expected not cancelled initially")
	}
	w.cancelled.Store(true)
	if !w.cancelled.Load() {
		t.Fatal("expected cancelled after store")
	}
}

func TestBackpressureErrors(t *testing.T) {
	if ErrSemaphoreClosed == nil {
		t.Fatal("ErrSemaphoreClosed must not be nil")
	}
	if ErrRateLimiterClosed == nil {
		t.Fatal("ErrRateLimiterClosed must not be nil")
	}
	if ErrThrottleClosed == nil {
		t.Fatal("ErrThrottleClosed must not be nil")
	}
}

func TestBackpressureMetrics(t *testing.T) {
	m := &backpressureMetrics{}
	m.RequestsTotal.Add(10)
	m.Accepted.Add(7)
	m.Rejected.Add(3)

	if m.RequestsTotal.Load() != 10 {
		t.Fatalf("expected 10 total, got %d", m.RequestsTotal.Load())
	}
	if m.Accepted.Load() != 7 {
		t.Fatalf("expected 7 accepted, got %d", m.Accepted.Load())
	}
	if m.Rejected.Load() != 3 {
		t.Fatalf("expected 3 rejected, got %d", m.Rejected.Load())
	}
}

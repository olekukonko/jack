package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSemaphoreBasicAcquireRelease(t *testing.T) {
	s := NewSemaphore(2)
	defer s.Close()

	if !s.TryAcquire(PriorityHigh) {
		t.Fatal("expected first acquire to succeed")
	}
	if !s.TryAcquire(PriorityHigh) {
		t.Fatal("expected second acquire to succeed")
	}
	if s.TryAcquire(PriorityHigh) {
		t.Fatal("expected third acquire to fail")
	}

	s.Release()
	if !s.TryAcquire(PriorityHigh) {
		t.Fatal("expected acquire after release to succeed")
	}
}

func TestSemaphoreBlockingAcquire(t *testing.T) {
	s := NewSemaphore(1)
	defer s.Close()

	if !s.TryAcquire(PriorityHigh) {
		t.Fatal("expected initial acquire")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Acquire(ctx, PriorityHigh); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		s.Release()
	}()

	time.Sleep(50 * time.Millisecond)
	s.Release()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for blocked acquire")
	}
}

func TestSemaphorePriorityOrdering(t *testing.T) {
	s := NewSemaphore(1)
	defer s.Close()

	if !s.TryAcquire(PriorityLow) {
		t.Fatal("expected initial acquire")
	}

	var order []Priority
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, p := range []Priority{PriorityLow, PriorityMedium, PriorityHigh, PriorityCritical} {
		wg.Add(1)
		go func(prio Priority) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := s.Acquire(ctx, prio); err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			mu.Lock()
			order = append(order, prio)
			mu.Unlock()
			s.Release()
		}(p)
	}

	time.Sleep(100 * time.Millisecond)
	s.Release()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 {
		t.Fatalf("expected 4 acquisitions, got %d", len(order))
	}
	if order[0] != PriorityCritical {
		t.Fatalf("expected Critical first, got %v", order[0])
	}
	if order[1] != PriorityHigh {
		t.Fatalf("expected High second, got %v", order[1])
	}
	if order[2] != PriorityMedium {
		t.Fatalf("expected Medium third, got %v", order[2])
	}
	if order[3] != PriorityLow {
		t.Fatalf("expected Low fourth, got %v", order[3])
	}
}

func TestSemaphoreContextCancellation(t *testing.T) {
	s := NewSemaphore(1)
	defer s.Close()

	if !s.TryAcquire(PriorityHigh) {
		t.Fatal("expected initial acquire")
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Acquire(ctx, PriorityHigh)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for cancellation")
	}
}

func TestSemaphoreClose(t *testing.T) {
	s := NewSemaphore(1)

	if !s.TryAcquire(PriorityHigh) {
		t.Fatal("expected initial acquire")
	}

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		errCh <- s.Acquire(ctx, PriorityHigh)
	}()

	time.Sleep(50 * time.Millisecond)
	s.Close()

	select {
	case err := <-errCh:
		if err != ErrSemaphoreClosed {
			t.Fatalf("expected ErrSemaphoreClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for close error")
	}
}

func TestSemaphoreMetrics(t *testing.T) {
	s := NewSemaphore(2)
	defer s.Close()

	s.TryAcquire(PriorityHigh)
	s.TryAcquire(PriorityHigh)
	s.Release()

	m := s.Metrics()
	if m.AcquiredFast.Load() != 2 {
		t.Fatalf("expected 2 fast acquires, got %d", m.AcquiredFast.Load())
	}
	if m.Released.Load() != 1 {
		t.Fatalf("expected 1 release, got %d", m.Released.Load())
	}
}

func TestSemaphoreConcurrentStress(t *testing.T) {
	s := NewSemaphore(10)
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := s.Acquire(ctx, PriorityHigh); err != nil {
					cancel()
					continue
				}
				cancel()
				time.Sleep(time.Microsecond)
				s.Release()
			}
		}()
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		if !s.TryAcquire(PriorityHigh) {
			t.Fatalf("expected slot %d to be available", i)
		}
	}
	if s.TryAcquire(PriorityHigh) {
		t.Fatal("expected all slots consumed")
	}
}

func TestSemaphoreCoDelOverload(t *testing.T) {
	// Small capacity, many waiters to trigger CoDel
	s := NewSemaphore(1,
		SemaphoreWithTargetSojourn(10*time.Millisecond),
		SemaphoreWithMaxSojourn(50*time.Millisecond),
	)
	defer s.Close()

	if !s.TryAcquire(PriorityLow) {
		t.Fatal("expected initial acquire")
	}

	var wg sync.WaitGroup
	acquired := make([]atomic.Bool, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			if err := s.Acquire(ctx, PriorityLow); err == nil {
				acquired[idx].Store(true)
				s.Release()
			}
		}(i)
	}

	// Hold the slot long enough to trigger CoDel dropping
	time.Sleep(100 * time.Millisecond)
	s.Release()
	wg.Wait()

	// Some should have been dropped due to maxSojourn
	timeouts := 0
	for i := 0; i < 20; i++ {
		if !acquired[i].Load() {
			timeouts++
		}
	}
	if timeouts == 0 {
		t.Fatal("expected some waiters to be dropped by CoDel")
	}
}

func TestSemaphoreInvalidPriority(t *testing.T) {
	s := NewSemaphore(2)
	defer s.Close()

	// Should not panic on invalid priority
	if !s.TryAcquire(Priority(-1)) {
		t.Fatal("expected TryAcquire with invalid priority")
	}
	if !s.TryAcquire(Priority(100)) {
		t.Fatal("expected TryAcquire with out-of-range priority")
	}
}

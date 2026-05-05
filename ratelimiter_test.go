package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimiterAllow(t *testing.T) {
	rl := NewRateLimiter(100, 10)
	defer rl.Close()

	for i := 0; i < 10; i++ {
		if !rl.Allow(PriorityHigh) {
			t.Fatalf("expected allow at iteration %d", i)
		}
	}
	if rl.Allow(PriorityHigh) {
		t.Fatal("expected deny after burst exhausted")
	}
}

func TestRateLimiterAllowN(t *testing.T) {
	rl := NewRateLimiter(100, 10)
	defer rl.Close()

	if !rl.AllowN(PriorityHigh, 5) {
		t.Fatal("expected AllowN(5) to succeed")
	}
	if !rl.AllowN(PriorityHigh, 5) {
		t.Fatal("expected AllowN(5) to succeed second time")
	}
	if rl.AllowN(PriorityHigh, 1) {
		t.Fatal("expected AllowN(1) to fail after burst")
	}
	if rl.AllowN(PriorityHigh, 0) {
		t.Fatal("expected AllowN(0) to fail")
	}
}

func TestRateLimiterRefill(t *testing.T) {
	rl := NewRateLimiter(10, 2)
	defer rl.Close()

	if !rl.Allow(PriorityHigh) || !rl.Allow(PriorityHigh) {
		t.Fatal("expected burst")
	}
	if rl.Allow(PriorityHigh) {
		t.Fatal("expected empty after burst")
	}

	time.Sleep(110 * time.Millisecond)
	if !rl.Allow(PriorityHigh) {
		t.Fatal("expected refill after 100ms")
	}
}

func TestRateLimiterBlockingAcquire(t *testing.T) {
	rl := NewRateLimiter(10, 1)
	defer rl.Close()

	if !rl.Allow(PriorityHigh) {
		t.Fatal("expected initial allow")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := rl.Acquire(ctx, PriorityHigh); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		rl.Release()
	}()

	time.Sleep(50 * time.Millisecond)
	rl.Release()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for blocked acquire")
	}
}

func TestRateLimiterPriorityOrdering(t *testing.T) {
	rl := NewRateLimiter(0, 1) // No refill, single token
	defer rl.Close()

	if !rl.Allow(PriorityLow) {
		t.Fatal("expected initial allow")
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
			if err := rl.Acquire(ctx, prio); err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			mu.Lock()
			order = append(order, prio)
			mu.Unlock()
			rl.Release()
		}(p)
	}

	time.Sleep(100 * time.Millisecond)
	rl.Release()
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

func TestRateLimiterContextCancellation(t *testing.T) {
	rl := NewRateLimiter(0, 1)
	defer rl.Close()

	if !rl.Allow(PriorityHigh) {
		t.Fatal("expected initial allow")
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- rl.Acquire(ctx, PriorityHigh)
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

func TestRateLimiterClose(t *testing.T) {
	rl := NewRateLimiter(100, 1)

	if !rl.Allow(PriorityHigh) {
		t.Fatal("expected initial allow")
	}

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		errCh <- rl.Acquire(ctx, PriorityHigh)
	}()

	time.Sleep(50 * time.Millisecond)
	rl.Close()

	select {
	case err := <-errCh:
		if err != ErrRateLimiterClosed {
			t.Fatalf("expected ErrRateLimiterClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for close error")
	}
}

func TestRateLimiterMetrics(t *testing.T) {
	rl := NewRateLimiter(100, 10)
	defer rl.Close()

	for i := 0; i < 5; i++ {
		rl.Allow(PriorityHigh)
	}

	m := rl.Metrics()
	if m.AllowedFast.Load() != 5 {
		t.Fatalf("expected 5 fast allows, got %d", m.AllowedFast.Load())
	}
	if m.TokensConsumed.Load() != 5 {
		t.Fatalf("expected 5 consumed, got %d", m.TokensConsumed.Load())
	}
}

func TestRateLimiterConcurrentStress(t *testing.T) {
	// Very low rate with small burst to guarantee contention
	rl := NewRateLimiter(1, 2)
	defer rl.Close()

	var wg sync.WaitGroup
	var allowed atomic.Uint64
	var denied atomic.Uint64

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if rl.Allow(PriorityHigh) {
					allowed.Add(1)
					time.Sleep(5 * time.Millisecond)
					rl.Release()
				} else {
					denied.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if allowed.Load() == 0 {
		t.Fatal("expected some allows")
	}
	if denied.Load() == 0 {
		t.Fatal("expected some denials under contention")
	}
}

func TestRateLimiterInvalidPriority(t *testing.T) {
	rl := NewRateLimiter(100, 2)
	defer rl.Close()

	if !rl.Allow(Priority(-1)) {
		t.Fatal("expected Allow with invalid priority")
	}
	if !rl.Allow(Priority(100)) {
		t.Fatal("expected Allow with out-of-range priority")
	}
}

func TestRateLimiterOptions(t *testing.T) {
	rl := NewRateLimiter(100, 10,
		RateLimiterWithMaxWait(200*time.Millisecond),
	)
	defer rl.Close()

	if rl.maxWait != 200*time.Millisecond {
		t.Fatalf("expected maxWait 200ms, got %v", rl.maxWait)
	}
}

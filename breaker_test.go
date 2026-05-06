package jack

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var errUpstream = errors.New("upstream error")

func TestBreakerInitialState(t *testing.T) {
	b := NewBreaker("test")
	if b.State() != BreakerClosed {
		t.Fatalf("expected Closed, got %v", b.State())
	}
}

func TestBreakerOpensAfterThreshold(t *testing.T) {
	b := NewBreaker("test", BreakerWithThreshold(3))
	ctx := context.Background()
	fail := func(context.Context) error { return errUpstream }

	for i := 0; i < 3; i++ {
		b.Call(ctx, fail) //nolint:errcheck
	}
	if b.State() != BreakerOpen {
		t.Fatalf("expected Open after threshold, got %v", b.State())
	}
}

func TestBreakerRejectsWhenOpen(t *testing.T) {
	b := NewBreaker("test", BreakerWithThreshold(1))
	ctx := context.Background()
	b.Call(ctx, func(context.Context) error { return errUpstream }) //nolint:errcheck

	err := b.Call(ctx, func(context.Context) error { return nil })
	if !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("expected ErrBreakerOpen, got %v", err)
	}
}

func TestBreakerHalfOpenAfterTimeout(t *testing.T) {
	b := NewBreaker("test",
		BreakerWithThreshold(1),
		BreakerWithOpenTimeout(50*time.Millisecond),
	)
	ctx := context.Background()
	b.Call(ctx, func(context.Context) error { return errUpstream }) //nolint:errcheck

	time.Sleep(60 * time.Millisecond)

	// Should transition to half-open on next allow check.
	err := b.Call(ctx, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("expected call to succeed in half-open, got %v", err)
	}
}

func TestBreakerClosesAfterSuccessThreshold(t *testing.T) {
	b := NewBreaker("test",
		BreakerWithThreshold(1),
		BreakerWithOpenTimeout(20*time.Millisecond),
		BreakerWithSuccessThreshold(2),
		BreakerWithHalfOpenLimit(2),
	)
	ctx := context.Background()
	b.Call(ctx, func(context.Context) error { return errUpstream }) //nolint:errcheck

	time.Sleep(30 * time.Millisecond)

	b.Call(ctx, func(context.Context) error { return nil }) //nolint:errcheck
	b.Call(ctx, func(context.Context) error { return nil }) //nolint:errcheck

	if b.State() != BreakerClosed {
		t.Fatalf("expected Closed after success threshold, got %v", b.State())
	}
}

func TestBreakerReopensOnHalfOpenFailure(t *testing.T) {
	b := NewBreaker("test",
		BreakerWithThreshold(1),
		BreakerWithOpenTimeout(20*time.Millisecond),
	)
	ctx := context.Background()
	b.Call(ctx, func(context.Context) error { return errUpstream }) //nolint:errcheck

	time.Sleep(30 * time.Millisecond)

	b.Call(ctx, func(context.Context) error { return errUpstream }) //nolint:errcheck
	if b.State() != BreakerOpen {
		t.Fatalf("expected Open after half-open failure, got %v", b.State())
	}
}

func TestBreakerReset(t *testing.T) {
	b := NewBreaker("test", BreakerWithThreshold(1))
	ctx := context.Background()
	b.Call(ctx, func(context.Context) error { return errUpstream }) //nolint:errcheck
	b.Reset()

	if b.State() != BreakerClosed {
		t.Fatalf("expected Closed after Reset, got %v", b.State())
	}
	if err := b.Call(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("expected call to succeed after Reset, got %v", err)
	}
}

func TestBreakerMetrics(t *testing.T) {
	b := NewBreaker("test", BreakerWithThreshold(5))
	ctx := context.Background()

	b.Call(ctx, func(context.Context) error { return nil })         //nolint:errcheck
	b.Call(ctx, func(context.Context) error { return errUpstream }) //nolint:errcheck

	m := b.Metrics()
	if m.Requests.Load() != 2 {
		t.Fatalf("expected 2 requests, got %d", m.Requests.Load())
	}
	if m.Successes.Load() != 1 {
		t.Fatalf("expected 1 success, got %d", m.Successes.Load())
	}
	if m.Failures.Load() != 1 {
		t.Fatalf("expected 1 failure, got %d", m.Failures.Load())
	}
}

func TestBreakerOnStateChange(t *testing.T) {
	var changes []BreakerState
	var mu sync.Mutex

	b := NewBreaker("test",
		BreakerWithThreshold(1),
		BreakerWithOnStateChange(func(_ string, _, to BreakerState) {
			mu.Lock()
			changes = append(changes, to)
			mu.Unlock()
		}),
	)
	ctx := context.Background()
	b.Call(ctx, func(context.Context) error { return errUpstream }) //nolint:errcheck

	mu.Lock()
	defer mu.Unlock()
	if len(changes) == 0 || changes[0] != BreakerOpen {
		t.Fatalf("expected Open transition, got %v", changes)
	}
}

func TestBreakerClosedInstance(t *testing.T) {
	b := NewBreaker("test")
	b.Close()

	err := b.Call(context.Background(), func(context.Context) error { return nil })
	if !errors.Is(err, ErrBreakerClosed) {
		t.Fatalf("expected ErrBreakerClosed, got %v", err)
	}
}

func TestBreakerConcurrentStress(t *testing.T) {
	b := NewBreaker("test",
		BreakerWithThreshold(50),
		BreakerWithOpenTimeout(10*time.Millisecond),
		BreakerWithHalfOpenLimit(5),
	)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b.Call(ctx, func(context.Context) error { //nolint:errcheck
					if j%3 == 0 {
						return errUpstream
					}
					return nil
				})
			}
		}(i)
	}
	wg.Wait()

	m := b.Metrics()
	if m.Requests.Load() == 0 {
		t.Fatal("expected non-zero requests")
	}
}

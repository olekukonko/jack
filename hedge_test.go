package jack

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestHedgerPrimaryWins(t *testing.T) {
	h := NewHedger(HedgeWithDelay(50 * time.Millisecond))

	var calls atomic.Int32
	val, err := h.Do(context.Background(), func(ctx context.Context) (any, error) {
		calls.Add(1)
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "ok" {
		t.Fatalf("unexpected value: %v", val)
	}
	if h.Metrics().Requests.Load() != 1 {
		t.Fatal("expected 1 request")
	}
}

func TestHedgerHedgeFires(t *testing.T) {
	h := NewHedger(HedgeWithDelay(20 * time.Millisecond))

	var calls atomic.Int32
	// Primary blocks; hedge fires and wins.
	val, err := h.Do(context.Background(), func(ctx context.Context) (any, error) {
		n := calls.Add(1)
		if n == 1 {
			// primary: block until cancelled
			<-ctx.Done()
			return nil, ctx.Err()
		}
		// hedge
		return "hedge", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hedge" {
		t.Fatalf("expected hedge to win, got %v", val)
	}
	if h.Metrics().HedgeWon.Load() != 1 {
		t.Fatalf("expected HedgeWon=1, got %d", h.Metrics().HedgeWon.Load())
	}
}

func TestHedgerBothFail(t *testing.T) {
	h := NewHedger(HedgeWithDelay(5 * time.Millisecond))

	_, err := h.Do(context.Background(), func(ctx context.Context) (any, error) {
		return nil, errors.New("fail")
	})
	if !errors.Is(err, ErrHedgeAllFailed) {
		t.Fatalf("expected ErrHedgeAllFailed, got %v", err)
	}
	if h.Metrics().Errors.Load() != 1 {
		t.Fatal("expected Errors=1")
	}
}

func TestHedgerContextCancellation(t *testing.T) {
	h := NewHedger(HedgeWithDelay(100 * time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := h.Do(ctx, func(ctx context.Context) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

func TestHedgerNoHedgeWhenPrimaryFast(t *testing.T) {
	h := NewHedger(HedgeWithDelay(200 * time.Millisecond))

	var calls atomic.Int32
	h.Do(context.Background(), func(ctx context.Context) (any, error) { //nolint:errcheck
		calls.Add(1)
		return "fast", nil
	})

	// Hedge timer should not have fired for a fast primary.
	time.Sleep(250 * time.Millisecond)
	if calls.Load() > 1 {
		t.Fatalf("expected 1 call (primary only), got %d", calls.Load())
	}
	if h.Metrics().Hedged.Load() != 0 {
		t.Fatalf("expected no hedge fired, got %d", h.Metrics().Hedged.Load())
	}
}

func TestHedgerAdaptiveDelay(t *testing.T) {
	h := NewHedger(
		HedgeWithPercentile(50),
		HedgeWithMinSamples(3),
	)

	// Warm up the RTT window with fast calls.
	for i := 0; i < 5; i++ {
		h.Do(context.Background(), func(ctx context.Context) (any, error) { //nolint:errcheck
			time.Sleep(5 * time.Millisecond)
			return nil, nil
		})
	}

	// With p50 ≈ 5ms, the hedge should fire for a slow primary.
	var calls atomic.Int32
	h.Do(context.Background(), func(ctx context.Context) (any, error) { //nolint:errcheck
		n := calls.Add(1)
		if n == 1 {
			time.Sleep(50 * time.Millisecond)
			return "primary", nil
		}
		return "hedge", nil
	})

	if h.Metrics().Hedged.Load() == 0 {
		t.Fatal("expected adaptive hedge to fire based on p50 RTT")
	}
}

func TestHedgerTyped(t *testing.T) {
	h := NewHedgerOf[string](HedgeWithDelay(50 * time.Millisecond))

	val, err := h.Do(context.Background(), func(ctx context.Context) (string, error) {
		return "typed", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "typed" {
		t.Fatalf("expected 'typed', got %q", val)
	}
}

func TestHedgerMetrics(t *testing.T) {
	h := NewHedger(HedgeWithDelay(5 * time.Millisecond))

	for i := 0; i < 5; i++ {
		h.Do(context.Background(), func(ctx context.Context) (any, error) { //nolint:errcheck
			return i, nil
		})
	}

	if h.Metrics().Requests.Load() != 5 {
		t.Fatalf("expected 5 requests, got %d", h.Metrics().Requests.Load())
	}
}

func TestHedgeGroup(t *testing.T) {
	h1 := NewHedger(HedgeWithDelay(5 * time.Millisecond))
	h2 := NewHedger(HedgeWithDelay(5 * time.Millisecond))
	g := NewHedgeGroup(h1, h2)

	var calls atomic.Int32
	val, err := g.Do(context.Background(), func(ctx context.Context) (any, error) {
		calls.Add(1)
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "ok" {
		t.Fatalf("unexpected value: %v", val)
	}
}

func TestHedgerConcurrentStress(t *testing.T) {
	h := NewHedger(
		HedgeWithDelay(10*time.Millisecond),
		HedgeWithMaxConcurrent(20),
	)
	var wg atomic.Int32
	wg.Store(50)
	done := make(chan struct{})

	for i := 0; i < 50; i++ {
		go func() {
			h.Do(context.Background(), func(ctx context.Context) (any, error) { //nolint:errcheck
				time.Sleep(time.Millisecond)
				return nil, nil
			})
			if wg.Add(-1) == 0 {
				close(done)
			}
		}()
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout in concurrent stress")
	}
	if h.Metrics().Requests.Load() != 50 {
		t.Fatalf("expected 50 requests, got %d", h.Metrics().Requests.Load())
	}
}

package jack

import (
	"context"
	"testing"
	"time"
)

func TestReservationImmediateGrant(t *testing.T) {
	rl := NewRateLimiter(100, 5)
	defer rl.Close()

	res := rl.Reserve(1)
	if !res.OK() {
		t.Fatal("expected valid reservation")
	}
	if res.Delay() != 0 {
		t.Fatalf("expected zero delay, got %v", res.Delay())
	}
	if err := res.Wait(context.Background()); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
}

func TestReservationDelay(t *testing.T) {
	rl := NewRateLimiter(2, 1) // 2 tokens/sec, burst 1
	defer rl.Close()

	// Exhaust the burst.
	rl.Allow(PriorityHigh)

	res := rl.Reserve(1)
	if !res.OK() {
		t.Fatal("expected valid reservation")
	}
	delay := res.Delay()
	if delay <= 0 {
		t.Fatalf("expected positive delay when bucket empty, got %v", delay)
	}
	res.Cancel()
}

func TestReservationCancel(t *testing.T) {
	rl := NewRateLimiter(1, 5)
	defer rl.Close()

	before := rl.Tokens()
	res := rl.Reserve(2)
	res.Cancel()

	// Tokens should be returned.
	after := rl.Tokens()
	if after != before {
		t.Fatalf("expected tokens restored after cancel: before=%d after=%d", before, after)
	}

	m := rl.ReserveMetrics()
	if m == nil {
		t.Fatal("expected non-nil reserve metrics")
	}
	if m.Dropped.Load() != 1 {
		t.Fatalf("expected 1 dropped, got %d", m.Dropped.Load())
	}
}

func TestReservationDoubleCancel(t *testing.T) {
	rl := NewRateLimiter(10, 5)
	defer rl.Close()

	res := rl.Reserve(1)
	res.Cancel()
	before := rl.Tokens()
	res.Cancel() // should be a no-op
	after := rl.Tokens()
	if before != after {
		t.Fatal("double cancel should not return tokens twice")
	}
}

func TestReservationWaitContextCancelled(t *testing.T) {
	rl := NewRateLimiter(0.1, 1) // very slow refill
	defer rl.Close()

	rl.Allow(PriorityHigh) // exhaust burst
	res := rl.Reserve(1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := res.Wait(ctx)
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

func TestReservationMaxDelay(t *testing.T) {
	rl := NewRateLimiter(1, 1) // 1 token/sec
	defer rl.Close()

	rl.Allow(PriorityHigh) // exhaust burst

	res := rl.Reserve(1, ReserveWithMaxDelay(10*time.Millisecond))
	if res.OK() {
		t.Fatal("expected cancelled reservation when delay exceeds maxDelay")
	}
	if res.Delay() != -1 {
		t.Fatalf("expected Delay==-1 for cancelled, got %v", res.Delay())
	}
}

func TestReservationMetrics(t *testing.T) {
	rl := NewRateLimiter(100, 10)
	defer rl.Close()

	for i := 0; i < 3; i++ {
		res := rl.Reserve(1)
		res.Wait(context.Background()) //nolint:errcheck
	}

	m := rl.ReserveMetrics()
	if m.Reserved.Load() != 3 {
		t.Fatalf("expected 3 reserved, got %d", m.Reserved.Load())
	}
	if m.Used.Load() != 3 {
		t.Fatalf("expected 3 used, got %d", m.Used.Load())
	}
}

func TestReservationNoBurst(t *testing.T) {
	rl := NewRateLimiter(0, 1) // no refill
	defer rl.Close()

	rl.Allow(PriorityHigh) // empty the bucket

	res := rl.Reserve(1)
	// With no refill, readyAt == now, so Delay should be 0 or slightly negative.
	if res.Delay() > time.Millisecond {
		t.Fatalf("expected near-zero delay with no refill, got %v", res.Delay())
	}
	// The reservation is technically valid even if no tokens will ever arrive
	// via time — the caller decides what to do.

}

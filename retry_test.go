package jack

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetrySuccessFirstAttempt(t *testing.T) {
	p := NewRetry()
	err := p.Do(context.Background(), func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if p.Metrics().Attempts.Load() != 1 {
		t.Fatalf("expected 1 attempt, got %d", p.Metrics().Attempts.Load())
	}
}

func TestRetrySuccessAfterRetries(t *testing.T) {
	p := NewRetry(
		RetryWithMaxAttempts(5),
		RetryWithBaseDelay(0),
	)
	var calls atomic.Int32
	err := p.Do(context.Background(), func(context.Context) error {
		if calls.Add(1) < 3 {
			return errUpstream
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 calls, got %d", calls.Load())
	}
}

func TestRetryExhausted(t *testing.T) {
	p := NewRetry(
		RetryWithMaxAttempts(3),
		RetryWithBaseDelay(0),
	)
	err := p.Do(context.Background(), func(context.Context) error { return errUpstream })
	if !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("expected ErrRetryExhausted, got %v", err)
	}
	if !errors.Is(err, errUpstream) {
		t.Fatalf("expected wrapped errUpstream in chain")
	}
	if p.Metrics().Failures.Load() != 1 {
		t.Fatalf("expected 1 failure metric, got %d", p.Metrics().Failures.Load())
	}
}

func TestRetryContextCancellation(t *testing.T) {
	p := NewRetry(RetryWithBaseDelay(50 * time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := p.Do(ctx, func(context.Context) error { return errUpstream })
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

func TestRetryPermanentError(t *testing.T) {
	permanent := errors.New("permanent")
	p := NewRetry(
		RetryWithMaxAttempts(5),
		RetryWithBaseDelay(0),
		RetryWithRetryIf(func(err error) bool { return !errors.Is(err, permanent) }),
	)
	var calls atomic.Int32
	err := p.Do(context.Background(), func(context.Context) error {
		calls.Add(1)
		return permanent
	})
	if err != permanent {
		t.Fatalf("expected permanent error returned directly, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call (no retry), got %d", calls.Load())
	}
}

func TestRetryOnRetryCallback(t *testing.T) {
	var attempts []int
	p := NewRetry(
		RetryWithMaxAttempts(3),
		RetryWithBaseDelay(0),
		RetryWithOnRetry(func(attempt int, _ error) {
			attempts = append(attempts, attempt)
		}),
	)
	p.Do(context.Background(), func(context.Context) error { return errUpstream }) //nolint:errcheck
	if len(attempts) != 2 {
		t.Fatalf("expected 2 onRetry callbacks, got %d", len(attempts))
	}
}

func TestRetryJitter(t *testing.T) {
	p := NewRetry(
		RetryWithBaseDelay(10*time.Millisecond),
		RetryWithMaxDelay(50*time.Millisecond),
		RetryWithJitter(true),
		RetryWithMaxAttempts(2),
	)
	start := time.Now()
	p.Do(context.Background(), func(context.Context) error { return errUpstream }) //nolint:errcheck
	elapsed := time.Since(start)
	// With jitter the delay is in [0, 10ms], so total should be well under 50ms.
	if elapsed > 100*time.Millisecond {
		t.Fatalf("jitter delay too long: %v", elapsed)
	}
}

func TestRetryMetrics(t *testing.T) {
	p := NewRetry(RetryWithMaxAttempts(3), RetryWithBaseDelay(0))
	p.Do(context.Background(), func(context.Context) error { return nil })         //nolint:errcheck
	p.Do(context.Background(), func(context.Context) error { return errUpstream }) //nolint:errcheck

	m := p.Metrics()
	if m.Successes.Load() != 1 {
		t.Fatalf("expected 1 success, got %d", m.Successes.Load())
	}
	if m.Failures.Load() != 1 {
		t.Fatalf("expected 1 failure, got %d", m.Failures.Load())
	}
}

func TestRetryNoJitter(t *testing.T) {
	p := NewRetry(
		RetryWithBaseDelay(1*time.Millisecond),
		RetryWithJitter(false),
		RetryWithMaxAttempts(2),
	)
	start := time.Now()
	p.Do(context.Background(), func(context.Context) error { return errUpstream }) //nolint:errcheck
	if time.Since(start) < time.Millisecond {
		t.Fatal("expected at least 1ms delay without jitter")
	}
}

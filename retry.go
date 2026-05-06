package jack

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

var (
	ErrRetryExhausted = errors.New("retry attempts exhausted")
)

// RetryMetrics tracks retry operational statistics.
type RetryMetrics struct {
	Attempts  atomic.Uint64 // total individual calls (including first)
	Successes atomic.Uint64 // calls that ultimately succeeded
	Failures  atomic.Uint64 // calls that exhausted all retries
	Timeouts  atomic.Uint64 // attempts abandoned due to context deadline
}

// RetryOption configures a Retry.
type RetryOption func(*Retry)

// RetryWithMaxAttempts sets the total number of attempts (initial + retries).
// Default is 3.
func RetryWithMaxAttempts(n int) RetryOption {
	return func(p *Retry) {
		if n > 0 {
			p.maxAttempts = n
		}
	}
}

// RetryWithBaseDelay sets the initial delay before the first retry (default 100ms).
func RetryWithBaseDelay(d time.Duration) RetryOption {
	return func(p *Retry) {
		if d >= 0 {
			p.baseDelay = d
		}
	}
}

// RetryWithMaxDelay caps the backoff delay (default 30s).
func RetryWithMaxDelay(d time.Duration) RetryOption {
	return func(p *Retry) {
		if d > 0 {
			p.maxDelay = d
		}
	}
}

// RetryWithMultiplier sets the exponential backoff multiplier (default 2.0).
func RetryWithMultiplier(m float64) RetryOption {
	return func(p *Retry) {
		if m >= 1 {
			p.multiplier = m
		}
	}
}

// RetryWithJitter enables full jitter on the backoff delay (default true).
// Full jitter randomises each delay in [0, computed_delay] to spread load.
func RetryWithJitter(enabled bool) RetryOption {
	return func(p *Retry) { p.jitter = enabled }
}

// RetryWithRetryIf replaces the default retry predicate.
// By default all non-nil errors are retried. Supply a custom function to skip
// retrying on permanent errors (e.g. 404, validation failures).
func RetryWithRetryIf(fn func(error) bool) RetryOption {
	return func(p *Retry) {
		if fn != nil {
			p.retryIf = fn
		}
	}
}

// RetryWithOnRetry registers a callback invoked before each retry with the
// attempt number (1-based) and the error that triggered the retry.
func RetryWithOnRetry(fn func(attempt int, err error)) RetryOption {
	return func(p *Retry) { p.onRetry = fn }
}

// Retry defines how retries are performed. Create one once and reuse it
// across many Do / DoCtx calls — it is safe for concurrent use.
type  struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
	multiplier  float64
	jitter      bool
	retryIf     func(error) bool
	onRetry     func(attempt int, err error)
	metrics     *RetryMetrics
}

// NewRetry constructs a Retry with sensible defaults.
func NewRetry(opts ...RetryOption) *Retry {
	p := &Retry{
		maxAttempts: 3,
		baseDelay:   100 * time.Millisecond,
		maxDelay:    30 * time.Second,
		multiplier:  2.0,
		jitter:      true,
		retryIf:     func(err error) bool { return err != nil },
		metrics:     &RetryMetrics{},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Metrics returns the policy's operational metrics.
func (p *Retry) Metrics() *RetryMetrics { return p.metrics }

// Do executes fn according to the policy, retrying on retryable errors.
// The provided context governs the total deadline; each individual attempt
// also respects context cancellation.
// Returns the last error wrapped in ErrRetryExhausted if all attempts fail.
func (p *Retry) Do(ctx context.Context, fn func(context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < p.maxAttempts; attempt++ {
		if ctx.Err() != nil {
			p.metrics.Timeouts.Add(1)
			p.metrics.Failures.Add(1)
			return ctx.Err()
		}

		p.metrics.Attempts.Add(1)
		lastErr = fn(ctx)

		if lastErr == nil {
			p.metrics.Successes.Add(1)
			return nil
		}

		if isTimeout(lastErr) {
			p.metrics.Timeouts.Add(1)
		}

		if !p.retryIf(lastErr) {
			p.metrics.Failures.Add(1)
			return lastErr
		}

		if attempt == p.maxAttempts-1 {
			break
		}

		delay := p.delay(attempt)
		if p.onRetry != nil {
			p.onRetry(attempt+1, lastErr)
		}

		select {
		case <-ctx.Done():
			p.metrics.Timeouts.Add(1)
			p.metrics.Failures.Add(1)
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	p.metrics.Failures.Add(1)
	return errors.Join(ErrRetryExhausted, lastErr)
}

// delay computes the backoff duration for a given attempt index (0-based).
func (p *Retry) delay(attempt int) time.Duration {
	d := float64(p.baseDelay) * math.Pow(p.multiplier, float64(attempt))
	if p.maxDelay > 0 && time.Duration(d) > p.maxDelay {
		d = float64(p.maxDelay)
	}
	if p.jitter {
		d = rand.Float64() * d // full jitter: uniform [0, d]
	}
	return time.Duration(d)
}

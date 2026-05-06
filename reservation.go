package jack

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrReservationExpired   = errors.New("reservation expired")
	ErrReservationCancelled = errors.New("reservation cancelled")
)

// ReservationMetrics tracks reservation operational statistics.
type ReservationMetrics struct {
	Reserved atomic.Uint64 // successful Reserve calls
	Used     atomic.Uint64 // reservations that called Wait successfully
	Dropped  atomic.Uint64 // reservations cancelled before use
	Expired  atomic.Uint64 // reservations that timed out during Wait
}

// Reservation is a future token grant from a RateLimiter.
// The caller inspects Delay() and decides whether to wait or drop.
// Once committed, call Wait(ctx) to block for exactly the reservation's delay.
// A Reservation is single-use and not safe for concurrent access.
type Reservation struct {
	limiter   *RateLimiter
	tokens    int64
	readyAt   time.Time // when the tokens will be available
	cancelled atomic.Bool
	used      atomic.Bool
	metrics   *ReservationMetrics
}

// Delay returns how long the caller must wait before the reserved tokens are
// available. Zero means tokens are available immediately. A negative value
// means the reservation was cancelled.
func (r *Reservation) Delay() time.Duration {
	if r.cancelled.Load() {
		return -1
	}
	d := time.Until(r.readyAt)
	if d < 0 {
		return 0
	}
	return d
}

// OK reports whether the reservation is valid and not yet used or cancelled.
func (r *Reservation) OK() bool {
	return !r.cancelled.Load() && !r.used.Load()
}

// Cancel releases the reservation without consuming tokens.
// Safe to call multiple times.
func (r *Reservation) Cancel() {
	if r.cancelled.CompareAndSwap(false, true) {
		r.metrics.Dropped.Add(1)
		// Always return tokens. On the fast path readyAt == grant time so the
		// time.Now().Before check would be false even though tokens were taken;
		// on the slow path tokens are taken speculatively against future refill
		// and must also be returned. In both cases Replenish is the right action.
		r.limiter.Replenish(r.tokens)
	}
}

// Wait blocks until the reservation's delay elapses or the context is cancelled.
// Returns nil when the tokens are ready to use.
func (r *Reservation) Wait(ctx context.Context) error {
	if r.cancelled.Load() {
		return ErrReservationCancelled
	}
	if !r.used.CompareAndSwap(false, true) {
		return ErrReservationCancelled
	}
	delay := r.Delay()
	if delay <= 0 {
		r.metrics.Used.Add(1)
		return nil
	}
	select {
	case <-ctx.Done():
		r.metrics.Expired.Add(1)
		r.Cancel()
		return ctx.Err()
	case <-time.After(delay):
		r.metrics.Used.Add(1)
		return nil
	}
}

// ReserveOption configures reservation behaviour.
type ReserveOption func(*reserveConfig)

type reserveConfig struct {
	maxDelay time.Duration // caller's tolerance; returns cancelled if delay > maxDelay
}

// ReserveWithMaxDelay causes Reserve to return a cancelled Reservation if the
// computed delay exceeds d. Use to implement admission control at the call site
// without blocking any goroutine.
func ReserveWithMaxDelay(d time.Duration) ReserveOption {
	return func(c *reserveConfig) { c.maxDelay = d }
}

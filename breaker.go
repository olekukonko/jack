package jack

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// BreakerState represents the state of a circuit breaker.
type BreakerState int32

const (
	// BreakerClosed is the normal operating state — calls pass through.
	BreakerClosed BreakerState = iota
	// BreakerOpen means the circuit is tripped — calls are rejected immediately.
	BreakerOpen
	// BreakerHalfOpen means a limited probe is allowed to test recovery.
	BreakerHalfOpen
)

// String returns a human-readable label for the circuit state.
func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

var (
	ErrBreakerOpen   = errors.New("circuit breaker open")
	ErrBreakerClosed = errors.New("circuit breaker instance closed")
)

// BreakerMetrics tracks circuit breaker operational statistics.
type BreakerMetrics struct {
	Requests      atomic.Uint64 // total calls attempted
	Successes     atomic.Uint64 // calls that returned nil
	Failures      atomic.Uint64 // calls that returned a non-nil error
	Rejections    atomic.Uint64 // calls rejected while open
	Timeouts      atomic.Uint64 // calls that exceeded their deadline
	StateChanges  atomic.Uint64 // total transitions between states
	ConsecSuccess atomic.Int64  // consecutive successes in half-open
	ConsecFailure atomic.Int64  // consecutive failures in current window
}

// BreakerOption configures a Breaker.
type BreakerOption func(*Breaker)

// BreakerWithThreshold sets the number of consecutive failures required to open
// the circuit (default 5).
func BreakerWithThreshold(n uint64) BreakerOption {
	return func(b *Breaker) {
		if n > 0 {
			b.threshold = n
		}
	}
}

// BreakerWithSuccessThreshold sets how many consecutive successes in half-open
// state are required before the circuit closes again (default 2).
func BreakerWithSuccessThreshold(n uint64) BreakerOption {
	return func(b *Breaker) {
		if n > 0 {
			b.successThreshold = n
		}
	}
}

// BreakerWithOpenTimeout sets how long the circuit stays open before moving to
// half-open to probe recovery (default 10s).
func BreakerWithOpenTimeout(d time.Duration) BreakerOption {
	return func(b *Breaker) {
		if d > 0 {
			b.openTimeout = d
		}
	}
}

// BreakerWithHalfOpenLimit sets the maximum number of concurrent probes allowed
// in half-open state (default 1).
func BreakerWithHalfOpenLimit(n int64) BreakerOption {
	return func(b *Breaker) {
		if n > 0 {
			b.halfOpenLimit = n
		}
	}
}

// BreakerWithOnStateChange registers a callback invoked on every state transition.
func BreakerWithOnStateChange(fn func(name string, from, to BreakerState)) BreakerOption {
	return func(b *Breaker) { b.onStateChange = fn }
}

// Breaker implements the circuit-breaker pattern.
//
// States:
//
//	Closed     → normal; failures accumulate until threshold is reached.
//	Open       → tripped; all calls rejected for openTimeout, then → HalfOpen.
//	HalfOpen   → probe; up to halfOpenLimit calls pass through. Enough successes
//	             → Closed; any failure → Open.
//
// Call is the only entry point. It wraps the user function and manages all
// state transitions atomically without holding a mutex during execution.
type Breaker struct {
	name             string
	threshold        uint64
	successThreshold uint64
	openTimeout      time.Duration
	halfOpenLimit    int64
	onStateChange    func(name string, from, to BreakerState)

	state         atomic.Int32 // BreakerState
	openedAt      atomic.Int64 // unix nano when last opened
	halfOpenSlots atomic.Int64 // slots remaining for half-open probes
	metrics       *BreakerMetrics
	closed        atomic.Bool // instance lifecycle, not circuit state
}

// NewBreaker creates a Breaker with the given name and options.
func NewBreaker(name string, opts ...BreakerOption) *Breaker {
	b := &Breaker{
		name:             name,
		threshold:        5,
		successThreshold: 2,
		openTimeout:      10 * time.Second,
		halfOpenLimit:    1,
		metrics:          &BreakerMetrics{},
	}
	for _, opt := range opts {
		opt(b)
	}
	b.state.Store(int32(BreakerClosed))
	return b
}

// State returns the current circuit state.
func (b *Breaker) State() BreakerState {
	return BreakerState(b.state.Load())
}

// Metrics returns the breaker's operational metrics.
func (b *Breaker) Metrics() *BreakerMetrics {
	return b.metrics
}

// Name returns the breaker's identifier.
func (b *Breaker) Name() string { return b.name }

// Call executes fn if the circuit allows it, updating state based on the result.
// Returns ErrBreakerOpen if the circuit is open. The context is passed through
// to fn; if fn returns context.DeadlineExceeded or context.Canceled the call
// counts as a timeout (also a failure).
func (b *Breaker) Call(ctx context.Context, fn func(context.Context) error) error {
	if b.closed.Load() {
		return ErrBreakerClosed
	}

	b.metrics.Requests.Add(1)

	if err := b.allow(); err != nil {
		b.metrics.Rejections.Add(1)
		return err
	}

	err := fn(ctx)
	b.record(err)
	return err
}

// allow checks whether a call may proceed, advancing state when needed.
func (b *Breaker) allow() error {
	for {
		state := BreakerState(b.state.Load())
		switch state {
		case BreakerClosed:
			return nil
		case BreakerOpen:
			if time.Since(time.Unix(0, b.openedAt.Load())) >= b.openTimeout {
				// Store slots BEFORE transitioning so any goroutine that sees
				// HalfOpen state immediately finds a valid slot count.
				b.halfOpenSlots.Store(b.halfOpenLimit)
				b.transition(BreakerOpen, BreakerHalfOpen)
				continue
			}
			return ErrBreakerOpen
		case BreakerHalfOpen:
			if b.halfOpenSlots.Add(-1) >= 0 {
				return nil
			}
			b.halfOpenSlots.Add(1) // return the slot we took
			return ErrBreakerOpen
		}
	}
}

// record updates metrics and state based on fn's outcome.
func (b *Breaker) record(err error) {
	if err != nil {
		if isTimeout(err) {
			b.metrics.Timeouts.Add(1)
		}
		b.metrics.Failures.Add(1)
		b.metrics.ConsecSuccess.Store(0)
		failures := b.metrics.ConsecFailure.Add(1)

		state := BreakerState(b.state.Load())
		switch state {
		case BreakerClosed:
			if uint64(failures) >= b.threshold {
				if b.transition(BreakerClosed, BreakerOpen) {
					b.openedAt.Store(time.Now().UnixNano())
					b.metrics.ConsecFailure.Store(0)
				}
			}
		case BreakerHalfOpen:
			b.halfOpenSlots.Add(1) // return probe slot
			if b.transition(BreakerHalfOpen, BreakerOpen) {
				b.openedAt.Store(time.Now().UnixNano())
				b.metrics.ConsecFailure.Store(0)
				b.metrics.ConsecSuccess.Store(0)
			}
		}
		return
	}

	b.metrics.Successes.Add(1)
	b.metrics.ConsecFailure.Store(0)
	successes := b.metrics.ConsecSuccess.Add(1)

	state := BreakerState(b.state.Load())
	if state == BreakerHalfOpen && uint64(successes) >= b.successThreshold {
		if b.transition(BreakerHalfOpen, BreakerClosed) {
			b.metrics.ConsecSuccess.Store(0)
		}
	}
}

// transition atomically moves from one state to another.
// Returns true if the transition was applied (i.e. from matched current state).
func (b *Breaker) transition(from, to BreakerState) bool {
	if !b.state.CompareAndSwap(int32(from), int32(to)) {
		return false
	}
	b.metrics.StateChanges.Add(1)
	if b.onStateChange != nil {
		b.onStateChange(b.name, from, to)
	}
	return true
}

// Reset forces the circuit back to Closed and clears all counters.
// Use for testing or manual operator intervention.
func (b *Breaker) Reset() {
	b.state.Store(int32(BreakerClosed))
	b.metrics.ConsecFailure.Store(0)
	b.metrics.ConsecSuccess.Store(0)
	b.halfOpenSlots.Store(0)
}

// Close permanently disables this Breaker instance.
func (b *Breaker) Close() { b.closed.Store(true) }

// isTimeout reports whether err is a context deadline or cancellation error.
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

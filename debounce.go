package jack

import (
	"math"
	"sync"
	"time"
)

// DebouncerOption is a functional option for configuring the Debouncer.
// These options allow customization of delay, maximum calls, and maximum wait time.
// Use WithDebounceDelay (required), WithDebounceMaxCalls, and WithDebounceMaxWait to set behaviors.
type DebouncerOption func(*Debouncer)

const (
	// NoLimitCalls is the default for maxCalls, meaning no limit on the number of calls before execution.
	// Set to math.MaxInt to indicate unbounded call accumulation.
	NoLimitCalls = math.MaxInt
	// NoLimitWait is the default for maxWait, meaning no time limit before forced execution.
	// Set to time.Duration(math.MaxInt64) to disable the maximum wait enforcement.
	NoLimitWait = time.Duration(math.MaxInt64)
)

// WithDebounceDelay sets the delay period after the last call before execution.
// This is a required option; without it, the debouncer will not function correctly.
// The delay determines the inactivity period needed to trigger the debounced function.
func WithDebounceDelay(d time.Duration) DebouncerOption {
	return func(db *Debouncer) { db.delay = d }
}

// WithDebounceMaxCalls sets the maximum number of calls before immediate execution.
// By default, there is no limit on call accumulation using NoLimitCalls constant.
// Once this threshold is reached, the function executes regardless of remaining delay.
func WithDebounceMaxCalls(count int) DebouncerOption {
	return func(db *Debouncer) { db.maxCalls = count }
}

// WithDebounceMaxWait sets the maximum wait time before forced function execution.
// This check happens on each Do call, so total wait could be up to maxWait plus delay.
// A separate timer ensures execution after maxWait even if no new calls occur.
func WithDebounceMaxWait(limit time.Duration) DebouncerOption {
	return func(db *Debouncer) { db.maxWait = limit }
}

// WithDebouncerPool sets a custom pool for executing debounced functions asynchronously.
// If not provided via this option, the default package-level pool will be used instead.
// This allows control over goroutine scheduling and resource management for executions.
func WithDebouncerPool(pool *Pool) DebouncerOption {
	return func(db *Debouncer) { db.pool = pool }
}

// Debouncer groups calls to a function, executing only after a period of inactivity
// or when certain thresholds (max calls or max wait) are met.
// It uses a mutex for thread-safety and timers for delay and maxWait enforcement.
type Debouncer struct {
	mu        sync.Mutex
	calls     int
	lastCall  int64 // UnixNanos
	startWait int64 // UnixNanos
	closed    bool
	fn        func()

	// Configuration
	delay    time.Duration
	maxCalls int
	maxWait  time.Duration
	pool     *Pool

	// Pre-allocated lazy timers
	timer        *time.Timer
	maxWaitTimer *time.Timer
}

// NewDebouncer creates a new Debouncer instance configured with the given functional options.
// The WithDebounceDelay option is required; other options are optional with sensible defaults.
// Timers are initialized but stopped until first use; limits default to no restrictions.
func NewDebouncer(options ...DebouncerOption) *Debouncer {
	d := &Debouncer{
		maxWait:  NoLimitWait,
		maxCalls: NoLimitCalls,
	}
	for _, opt := range options {
		opt(d)
	}
	// d.pool is nil unless WithDebouncerPool was called — that's correct.
	// Do NOT fall back to a default pool here.

	d.timer = time.AfterFunc(d.delay, d.timerFired)
	d.timer.Stop()

	if d.maxWait != NoLimitWait {
		d.maxWaitTimer = time.AfterFunc(d.maxWait, d.maxWaitFired)
		d.maxWaitTimer.Stop()
	}

	return d
}

// Do schedules the given function to execute after the configured delay period of inactivity.
// Each call resets the delay timer and updates the pending function to the latest provided.
// If maxCalls or maxWait thresholds are met, execution happens immediately without waiting.
func (d *Debouncer) Do(fn func()) {
	if fn == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return
	}

	now := time.Now().UnixNano()
	d.fn = fn
	d.lastCall = now
	d.calls++

	// First call in a new burst starts the timers.
	// We AVOID expensive timer.Reset() calls on subsequent rapid calls.
	if d.calls == 1 {
		d.startWait = now
		d.timer.Reset(d.delay)
		if d.maxWaitTimer != nil {
			d.maxWaitTimer.Reset(d.maxWait)
		}
		return
	}

	// Threshold checks for immediate execution
	if d.maxCalls != NoLimitCalls && d.calls >= d.maxCalls {
		d.flushLocked()
		return
	}

	if d.maxWait != NoLimitWait && time.Duration(now-d.startWait) >= d.maxWait {
		d.flushLocked()
		return
	}
}

// Flush executes any pending debounced function immediately without waiting for timers.
// It stops all active timers, executes the function in a new goroutine, and resets state.
// If no function is currently pending, this method does nothing and returns safely.
func (d *Debouncer) Flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.flushLocked()
}

// Cancel prevents a pending debounced function from executing by clearing internal state.
// It stops all active timers and resets call counters without invoking the pending function.
// Safe to call multiple times or when no function is pending; has no effect if already closed.
func (d *Debouncer) Cancel() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return
	}

	d.fn = nil
	d.calls = 0
	d.timer.Stop()
	if d.maxWaitTimer != nil {
		d.maxWaitTimer.Stop()
	}
}

// Stop permanently shuts down the debouncer and prevents any future function executions.
// It sets the closed flag, clears pending functions, and stops all active timers immediately.
// After calling Stop, any subsequent Do calls will be ignored and no operations will proceed.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.closed = true
	d.fn = nil
	d.calls = 0
	d.timer.Stop()
	if d.maxWaitTimer != nil {
		d.maxWaitTimer.Stop()
	}
}

// IsPending returns true if a debounced function is currently waiting to be executed.
// This method checks the internal call counter while holding the mutex for thread safety.
// Useful for monitoring debouncer state without triggering any execution or side effects.
func (d *Debouncer) IsPending() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls > 0
}

// timerFired handles the delay timer expiration event and decides whether to execute or reset.
// It checks if sufficient time has passed since the last call to determine execution readiness.
// If a new call occurred recently, it resets the timer for the remaining delay period.
func (d *Debouncer) timerFired() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.calls == 0 || d.closed {
		return
	}

	now := time.Now().UnixNano()
	elapsed := time.Duration(now - d.lastCall)

	if elapsed >= d.delay {
		// Delay has passed completely since the last Do() call.
		d.flushLocked()
	} else {
		// Do() was called recently. Timer fired too early for the current burst.
		// Reset for the remaining delay time.
		d.timer.Reset(d.delay - elapsed)
	}
}

// maxWaitFired handles the maximum wait timer expiration and enforces forced execution.
// It verifies that the elapsed time since the first call has reached the configured limit.
// If the timer fired early due to race conditions, it resets for the remaining wait time.
func (d *Debouncer) maxWaitFired() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.calls == 0 || d.closed {
		return
	}

	now := time.Now().UnixNano()
	elapsed := time.Duration(now - d.startWait)

	if elapsed >= d.maxWait {
		d.flushLocked()
	} else {
		// Gracefully handle stale timer callbacks from a previous flush.
		d.maxWaitTimer.Reset(d.maxWait - elapsed)
	}
}

// flushLocked executes the pending function and resets all internal state while holding the mutex.
// It stops both timers, captures the function to execute, and clears call counters atomically.
// The function runs asynchronously via goroutine or custom pool if one was configured.
func (d *Debouncer) flushLocked() {
	if d.calls == 0 {
		return
	}

	fn := d.fn
	d.fn = nil
	d.calls = 0

	d.timer.Stop()
	if d.maxWaitTimer != nil {
		d.maxWaitTimer.Stop()
	}

	// Use pool only if explicitly set via WithDebouncerPool.
	// go fn() costs 0 allocs — the scheduler reuses goroutine stacks.
	// pool.Do(fn) allocates a task struct every time, destroying the 0-alloc guarantee.
	if d.pool != nil {
		d.pool.Do(fn)
	} else {
		go fn()
	}
}

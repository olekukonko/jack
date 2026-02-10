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

// WithDebounceDelay sets the delay period after the last call before the function executes.
// This is a required option; without it, the debouncer will not function correctly.
// The delay determines the inactivity period needed to trigger execution.
func WithDebounceDelay(d time.Duration) DebouncerOption {
	return func(db *Debouncer) {
		db.delay = d
	}
}

// WithDebounceMaxCalls sets the maximum number of calls before the debounced function is executed.
// By default, there is no limit (NoLimitCalls).
// Once this threshold is reached, the function executes immediately, regardless of delay.
func WithDebounceMaxCalls(count int) DebouncerOption {
	return func(db *Debouncer) {
		db.maxCalls = count
	}
}

// WithDebounceMaxWait sets the maximum wait time before the debounced function is executed.
// This check happens on each Do call, so total maximum wait time could be up to (maxWait + delay) in some cases.
// A separate timer ensures execution after maxWait even if no new calls occur.
func WithDebounceMaxWait(limit time.Duration) DebouncerOption {
	return func(db *Debouncer) {
		db.maxWait = limit
	}
}

// Debouncer groups calls to a function, executing only after a period of inactivity
// or when certain thresholds (max calls or max wait) are met.
// It uses a mutex for thread-safety and timers for delay and maxWait enforcement.
// The last provided function in a series of Do calls is the one executed.
type Debouncer struct {
	mu           sync.Mutex
	delay        time.Duration
	timer        *time.Timer
	calls        int
	maxCalls     int
	startWait    time.Time
	maxWait      time.Duration
	maxWaitTimer *time.Timer
	// Stores last function to debounce.
	fn func()
}

// NewDebouncer creates a new Debouncer instance configured with the given options.
// The WithDebounceDelay option is required; others are optional.
// Timers are initialized but stopped until first use; maxWait and maxCalls default to no limits.
func NewDebouncer(options ...DebouncerOption) *Debouncer {
	db := &Debouncer{
		maxWait:  NoLimitWait,
		maxCalls: NoLimitCalls,
	}
	for _, opt := range options {
		opt(db)
	}
	// Initialize timers and immediately stop them to prepare for Reset calls.
	db.timer = time.AfterFunc(NoLimitWait, db.fire)
	db.timer.Stop()
	db.maxWaitTimer = time.AfterFunc(NoLimitWait, db.fireMaxWait)
	db.maxWaitTimer.Stop()
	return db
}

// Do schedules the given function to be executed after the configured delay.
// Each call to Do resets the delay timer and updates the function to the latest one provided.
// If maxCalls or maxWait thresholds are met, it executes immediately; otherwise, waits for delay.
// Starts maxWait timer on first call if configured.
func (d *Debouncer) Do(fn func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fn = fn
	if d.calls == 0 {
		d.startWait = time.Now()
		if d.maxWait != NoLimitWait {
			stopAndDrainTimer(d.maxWaitTimer)
			d.maxWaitTimer.Reset(d.maxWait)
		}
	}
	d.calls++
	if d.callLimitReached() || d.timeLimitReached() {
		d.flush()
	} else {
		stopAndDrainTimer(d.timer)
		d.timer.Reset(d.delay)
	}
}

// Cancel prevents a pending debounced function from executing.
// It stops all timers and resets internal state without executing the function.
// Safe to call multiple times or when no function is pending.
func (d *Debouncer) Cancel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	stopAndDrainTimer(d.timer)
	stopAndDrainTimer(d.maxWaitTimer)
	d.reset()
}

// Flush executes any pending debounced function immediately.
// It stops timers, executes the function in a new goroutine, and resets state.
// If no function is pending, it does nothing.
func (d *Debouncer) Flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.flush()
}

// fire is the internal method called by the delay timer.
// It acquires the lock and flushes the pending function.
// Ensures execution only after the delay has passed without new calls.
func (d *Debouncer) fire() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.flush()
}

// fireMaxWait is the internal method called by the maxWait timer.
// It acquires the lock and flushes only if a function is pending.
// Prevents indefinite waiting by enforcing the maxWait limit from the first call.
func (d *Debouncer) fireMaxWait() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fn != nil { // Only flush if there's a pending function
		d.flush()
	}
}

// flush executes the function and resets the state. Must be called within a mutex lock.
// Stops timers, captures the function, resets, and runs it in a goroutine to avoid blocking.
// Does nothing if no function is set.
func (d *Debouncer) flush() {
	if d.fn == nil {
		return // Nothing to flush
	}
	stopAndDrainTimer(d.timer)
	stopAndDrainTimer(d.maxWaitTimer)
	fn := d.fn
	d.reset()
	// Run the function in a new goroutine to avoid blocking the caller of Do/Flush.
	go fn()
}

// reset clears the state. Must be called within a mutex lock.
// Sets function to nil and resets call count.
// Prepares for the next series of debounced calls.
func (d *Debouncer) reset() {
	d.fn = nil
	d.calls = 0
}

// callLimitReached checks if the maximum call threshold has been met or exceeded.
// Returns true if maxCalls is set and calls >= maxCalls.
// Used internally to trigger immediate execution.
func (d *Debouncer) callLimitReached() bool {
	return d.maxCalls != NoLimitCalls && d.calls >= d.maxCalls
}

// timeLimitReached checks if the time since the first call exceeds maxWait.
// Returns true if maxWait is set and elapsed time >= maxWait.
// Evaluated on each Do call for timely enforcement.
func (d *Debouncer) timeLimitReached() bool {
	return d.maxWait != NoLimitWait && time.Since(d.startWait) >= d.maxWait
}

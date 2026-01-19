package jack

import (
	"context"
	"sync"
	"time"

	"github.com/olekukonko/ll"
)

// LifetimeHook defines a lifecycle hook function that can return an error.
// Used for Start hook only (matching zevent.Runner.Start).
type LifetimeHook func(ctx context.Context, id string) error

// LifetimeCallback defines a callback function without error return.
// Used for End and Timed hooks (matching zevent.Runner.End/Timed).
type LifetimeCallback func(ctx context.Context, id string)

// Lifetime defines lifecycle hooks for an operation, matching zevent.Runner exactly.
// No error propagation from End hook - 100% matches original zevent behavior.
type Lifetime struct {
	// Start runs before the operation begins.
	// If it returns an error, the operation is aborted.
	Start LifetimeHook

	// End runs after the operation completes successfully.
	// No error return - matches zevent.Runner.End exactly.
	End LifetimeCallback

	// Timed runs after a specific duration if not cancelled/reset.
	// Used for inactivity/TTL logic. No error return.
	Timed     LifetimeCallback
	TimedWait time.Duration
}

// LifetimeOption configures a Lifetime.
type LifetimeOption func(*Lifetime)

// LifetimeWithStart sets the Start hook.
func LifetimeWithStart(hook LifetimeHook) LifetimeOption {
	return func(l *Lifetime) {
		l.Start = hook
	}
}

// LifetimeWithEnd sets the End hook.
func LifetimeWithEnd(callback LifetimeCallback) LifetimeOption {
	return func(l *Lifetime) {
		l.End = callback
	}
}

// LifetimeWithTimed sets the Timed hook and wait duration.
func LifetimeWithTimed(callback LifetimeCallback, wait time.Duration) LifetimeOption {
	return func(l *Lifetime) {
		l.Timed = callback
		l.TimedWait = wait
	}
}

// NewLifetime creates a new Lifetime with the given options.
func NewLifetime(opts ...LifetimeOption) *Lifetime {
	l := &Lifetime{}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Execute runs the operation with the lifetime's hooks.
// 100% matches zevent.Runner.Execute behavior.
func (l *Lifetime) Execute(ctx context.Context, id string, operation Func) error {
	// Run Start hook
	if l.Start != nil {
		if err := l.Start(ctx, id); err != nil {
			return err
		}
	}

	// Execute the operation
	if err := operation(); err != nil {
		return err
	}

	// Run End hook (no error return - matches zevent)
	if l.End != nil {
		l.End(ctx, id)
	}

	return nil
}

// ExecuteCtx runs a context-aware operation with the lifetime's hooks.
// Jack extension - not in original zevent.
func (l *Lifetime) ExecuteCtx(ctx context.Context, id string, operation FuncCtx) error {
	// Run Start hook
	if l.Start != nil {
		if err := l.Start(ctx, id); err != nil {
			return err
		}
	}

	// Execute the operation
	if err := operation(ctx); err != nil {
		return err
	}

	// Run End hook (no error return - matches zevent)
	if l.End != nil {
		l.End(ctx, id)
	}

	return nil
}

// LifetimeManager manages multiple lifetimes and their timed events.
type LifetimeManager struct {
	timers   map[string]*activeLifetime
	timersMu sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// Configuration
	defaultWait  time.Duration
	defaultTimer time.Duration
	logger       *ll.Logger
}

// activeLifetime represents a pending timed callback.
type activeLifetime struct {
	timer    *time.Timer
	callback LifetimeCallback
	duration time.Duration
	id       string
}

// LifetimeManagerOption configures a LifetimeManager.
type LifetimeManagerOption func(*LifetimeManager)

// LifetimeManagerWithDefaultWait sets the default wait duration for timed callbacks.
func LifetimeManagerWithDefaultWait(wait time.Duration) LifetimeManagerOption {
	return func(lm *LifetimeManager) {
		if wait > 0 {
			lm.defaultWait = wait
		}
	}
}

// LifetimeManagerWithDefaultTimer sets the default timer duration for callbacks.
func LifetimeManagerWithDefaultTimer(timeout time.Duration) LifetimeManagerOption {
	return func(lm *LifetimeManager) {
		if timeout > 0 {
			lm.defaultTimer = timeout
		}
	}
}

// LifetimeManagerWithLogger sets a custom logger.
func LifetimeManagerWithLogger(l *ll.Logger) LifetimeManagerOption {
	return func(lm *LifetimeManager) {
		if l != nil {
			lm.logger = l.Namespace("lifetime")
		}
	}
}

// NewLifetimeManager creates a new LifetimeManager.
// Defaults: 30min wait, 1min timer timeout.
func NewLifetimeManager(opts ...LifetimeManagerOption) *LifetimeManager {
	ctx, cancel := context.WithCancel(context.Background())
	lm := &LifetimeManager{
		timers:       make(map[string]*activeLifetime),
		ctx:          ctx,
		cancel:       cancel,
		defaultWait:  30 * time.Minute,
		defaultTimer: 1 * time.Minute,
		logger:       logger.Namespace("lifetime"),
	}

	for _, opt := range opts {
		opt(lm)
	}

	return lm
}

// ScheduleTimed schedules or resets a timed callback for a given ID.
func (lm *LifetimeManager) ScheduleTimed(ctx context.Context, id string, callback LifetimeCallback, wait time.Duration) {
	if wait == 0 {
		wait = lm.defaultWait
	}
	if callback == nil {
		lm.logger.Debug("ScheduleTimed: nil callback for ID %s", id)
		return
	}

	lm.timersMu.Lock()

	// Stop existing timer if any
	if existing, ok := lm.timers[id]; ok {
		if !existing.timer.Stop() {
			// Timer already fired or stopped, drain the channel if needed
			select {
			case <-existing.timer.C:
			default:
			}
		}
		delete(lm.timers, id)
	}

	// Create new timer
	timer := time.NewTimer(wait)

	lm.timers[id] = &activeLifetime{
		timer:    timer,
		callback: callback,
		duration: wait,
		id:       id,
	}

	lm.timersMu.Unlock()

	lm.logger.Debug("ScheduleTimed: scheduled callback for ID %s in %v", id, wait)

	// Start goroutine to handle the timer
	lm.wg.Add(1)
	go func() {
		defer lm.wg.Done()

		select {
		case <-timer.C:
			// Timer fired, execute callback
			lm.timersMu.Lock()
			delete(lm.timers, id)
			lm.timersMu.Unlock()

			lm.logger.Debug("ScheduleTimed: executing callback for ID %s", id)

			// Execute callback with timeout
			bgCtx, cancel := context.WithTimeout(context.Background(), lm.defaultTimer)
			defer cancel()
			callback(bgCtx, id)

		case <-lm.ctx.Done():
			// Manager stopped, clean up
			timer.Stop()
			lm.logger.Debug("ScheduleTimed: manager stopped, cleaning up timer for ID %s", id)
		}
	}()
}

// ScheduleLifetimeTimed schedules or resets a timed callback from a Lifetime.
func (lm *LifetimeManager) ScheduleLifetimeTimed(ctx context.Context, id string, lifetime *Lifetime) {
	if lifetime == nil || lifetime.Timed == nil {
		// If no timed lifetime, just reset existing if any
		lm.ResetTimed(id)
		return
	}

	lm.ScheduleTimed(ctx, id, lifetime.Timed, lifetime.TimedWait)
}

// ExecuteWithLifetime executes an operation with lifetime hooks and manages timed events.
// 100% matches zevent.Manager.ExecuteWithRunner behavior.
func (lm *LifetimeManager) ExecuteWithLifetime(ctx context.Context, id string, lifetime *Lifetime, operation Func) error {
	if lifetime == nil {
		return operation()
	}

	// Execute with hooks
	err := lifetime.Execute(ctx, id, operation)

	// If successful and we have a Timed callback, schedule it
	if err == nil && lifetime.Timed != nil {
		lm.ScheduleLifetimeTimed(ctx, id, lifetime)
	}

	return err
}

// ExecuteCtxWithLifetime executes a context-aware operation with lifetime hooks.
// Jack extension - not in original zevent.
func (lm *LifetimeManager) ExecuteCtxWithLifetime(ctx context.Context, id string, lifetime *Lifetime, operation FuncCtx) error {
	if lifetime == nil {
		return operation(ctx)
	}

	// Execute with hooks
	err := lifetime.ExecuteCtx(ctx, id, operation)

	// If successful and we have a Timed callback, schedule it
	if err == nil && lifetime.Timed != nil {
		lm.ScheduleLifetimeTimed(ctx, id, lifetime)
	}

	return err
}

// ResetTimed resets the timer for an existing lifetime (Keep-Alive).
func (lm *LifetimeManager) ResetTimed(id string) bool {
	lm.timersMu.Lock()
	defer lm.timersMu.Unlock()

	if timer, exists := lm.timers[id]; exists {
		// Stop the timer first
		if !timer.timer.Stop() {
			// If Stop() returns false, the timer has already fired or been stopped
			delete(lm.timers, id)
			lm.logger.Debug("ResetTimed: timer already fired for ID %s", id)
			return false
		}

		// Reset the timer
		timer.timer.Reset(timer.duration)
		lm.logger.Debug("ResetTimed: reset timer for ID %s", id)
		return true
	}

	lm.logger.Debug("ResetTimed: no timer found for ID %s", id)
	return false
}

// CancelTimed cancels and removes a timed callback.
func (lm *LifetimeManager) CancelTimed(id string) bool {
	lm.timersMu.Lock()
	defer lm.timersMu.Unlock()

	if timer, ok := lm.timers[id]; ok {
		timer.timer.Stop()
		delete(lm.timers, id)
		lm.logger.Debug("CancelTimed: cancelled timer for ID %s", id)
		return true
	}

	lm.logger.Debug("CancelTimed: no timer found for ID %s", id)
	return false
}

// StopAll cancels all pending timed callbacks and waits for them to complete.
func (lm *LifetimeManager) StopAll() {
	lm.logger.Info("StopAll: stopping all timers")

	// Cancel context to signal all goroutines
	lm.cancel()

	// Stop all timers
	lm.timersMu.Lock()
	for id, timer := range lm.timers {
		timer.timer.Stop()
		delete(lm.timers, id)
	}
	lm.timersMu.Unlock()

	// Wait for all goroutines to complete
	lm.wg.Wait()

	lm.logger.Info("StopAll: all timers stopped")
}

// HasPending checks if there's a pending timed callback for an ID.
func (lm *LifetimeManager) HasPending(id string) bool {
	lm.timersMu.RLock()
	_, exists := lm.timers[id]
	lm.timersMu.RUnlock()
	return exists
}

// PendingCount returns the number of pending timed callbacks.
func (lm *LifetimeManager) PendingCount() int {
	lm.timersMu.RLock()
	count := len(lm.timers)
	lm.timersMu.RUnlock()
	return count
}

// GetRemainingDuration returns the configured duration for a pending callback.
func (lm *LifetimeManager) GetRemainingDuration(id string) (time.Duration, bool) {
	lm.timersMu.RLock()
	timer, exists := lm.timers[id]
	lm.timersMu.RUnlock()

	if !exists || timer == nil {
		return 0, false
	}

	return timer.duration, true
}

// Stop gracefully stops the LifetimeManager.
func (lm *LifetimeManager) Stop() {
	lm.StopAll()
}

// RunWithLifetime is a convenience function that creates and executes a lifetime.
func RunWithLifetime(ctx context.Context, id string, operation Func, opts ...LifetimeOption) error {
	lifetime := NewLifetime(opts...)
	return lifetime.Execute(ctx, id, operation)
}

// RunCtxWithLifetime is a convenience function for context-aware operations.
func RunCtxWithLifetime(ctx context.Context, id string, operation FuncCtx, opts ...LifetimeOption) error {
	lifetime := NewLifetime(opts...)
	return lifetime.ExecuteCtx(ctx, id, operation)
}

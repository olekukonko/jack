package jack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/ll"
)

// ShutdownOption configures the Shutdown.
type ShutdownOption func(*Shutdown)

// ShutdownWithTimeout sets the maximum time to wait for shutdown completion.
// Used as the context timeout for callbacks.
func ShutdownWithTimeout(d time.Duration) ShutdownOption {
	return func(sm *Shutdown) {
		sm.timeout = d
	}
}

// ShutdownConcurrent enables concurrent execution of cleanup functions.
// By default, execution is sequential (LIFO).
func ShutdownConcurrent() ShutdownOption {
	return func(sm *Shutdown) {
		sm.concurrent = true
	}
}

// ShutdownWithSignals specifies which OS signals to capture.
// If not set, defaults to SIGINT, SIGTERM, and SIGQUIT.
func ShutdownWithSignals(signals ...os.Signal) ShutdownOption {
	return func(sm *Shutdown) {
		sm.signals = signals
	}
}

// ShutdownWithForceQuit enables a force quit trigger after a specific timeout.
// This triggers context cancellation if the shutdown takes too long.
func ShutdownWithForceQuit(d time.Duration) ShutdownOption {
	return func(sm *Shutdown) {
		sm.forceQuitTimeout = d
	}
}

// ShutdownWithLogger sets a custom logger for the manager.
func ShutdownWithLogger(l *ll.Logger) ShutdownOption {
	return func(sm *Shutdown) {
		if l != nil {
			sm.logger = l.Namespace("shutdown")
		}
	}
}

// Shutdown manages the graceful shutdown process.
type Shutdown struct {
	mu         sync.RWMutex
	signalChan chan os.Signal
	doneChan   chan struct{}
	forceQuit  chan struct{}
	// events stores the callbacks. We wrap everything into namedCall
	// to normalize execution logic.
	events      []namedCall
	inShutdown  atomic.Bool
	shutdownCtx context.Context
	cancelFunc  context.CancelFunc
	// Configuration
	timeout          time.Duration
	concurrent       bool
	signals          []os.Signal
	forceQuitTimeout time.Duration
	logger           *ll.Logger
	// Statistics
	statsMu sync.RWMutex
	stats   *ShutdownStats
}

type namedCall struct {
	Name string
	Fn   FuncCtx // Uses jack.FuncCtx (func(context.Context) error)
}

// ShutdownStats contains metrics about the shutdown execution.
type ShutdownStats struct {
	TotalEvents     int
	CompletedEvents int
	FailedEvents    int
	StartTime       time.Time
	EndTime         time.Time
	Errors          []error
}

// NewShutdown creates a configured manager.
// Defaults: 30s timeout, sequential execution, standard signals.
// Returns a ready-to-use Shutdown instance with signal handling set up.
func NewShutdown(opts ...ShutdownOption) *Shutdown {
	// Initialize with defaults
	sm := &Shutdown{
		timeout:    30 * time.Second,
		concurrent: false,
		signals: []os.Signal{
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT,
		},
		doneChan: make(chan struct{}),
		stats: &ShutdownStats{
			Errors: make([]error, 0),
		},
	}
	// Apply logger default (can be overridden by opts)
	if logger != nil {
		sm.logger = logger.Namespace("shutdown")
	} else {
		sm.logger = &ll.Logger{}
	}
	// Apply options
	for _, opt := range opts {
		opt(sm)
	}
	// Setup Signals
	sm.signalChan = make(chan os.Signal, 1)
	signal.Notify(sm.signalChan, sm.signals...)
	// Setup Context
	// We only apply the timeout during the actual shutdown phase.
	ctx, cancel := context.WithCancel(context.Background())
	sm.shutdownCtx = ctx
	sm.cancelFunc = cancel
	// Setup Force Quit Monitor
	if sm.forceQuitTimeout > 0 {
		sm.forceQuit = make(chan struct{}, 1)
		go sm.forceQuitMonitor(sm.forceQuitTimeout)
	}
	return sm
}

// forceQuitMonitor runs in background and forces context cancellation
// if the configured force-quit timeout is reached before normal completion.
// This ensures the process doesn't hang indefinitely on stuck cleanup tasks.
func (sm *Shutdown) forceQuitMonitor(timeout time.Duration) {
	select {
	case <-sm.shutdownCtx.Done():
		// Normal shutdown completed or main timeout reached first
		return
	case <-time.After(timeout):
		sm.log("force quit timeout (%v) reached — cancelling context", timeout)
		sm.cancelFunc()
		// Signal the Wait loop if it's blocking
		if sm.forceQuit != nil {
			select {
			case sm.forceQuit <- struct{}{}:
			default:
			}
		}
	}
}

// Register adds a cleanup task.
// Supported types:
//   - func(), func() error, func(context.Context) error, io.Closer
//
// Automatically wraps panics into structured errors and tracks stats.
func (sm *Shutdown) Register(fn any) error {
	if fn == nil {
		return errors.New("cannot register nil")
	}
	var name string
	var call FuncCtx
	switch f := fn.(type) {
	case func():
		name = autoName(f)
		call = func(ctx context.Context) error {
			f()
			return nil
		}
	case func() error: // jack.Func compatible
		name = autoName(f)
		call = func(ctx context.Context) error {
			return f()
		}
	case Func: // Explicit jack.Func
		name = autoName(f)
		call = func(ctx context.Context) error {
			return f()
		}
	case func(context.Context) error: // jack.FuncCtx compatible
		name = autoName(f)
		call = f
	case FuncCtx: // Explicit jack.FuncCtx
		name = autoName(f)
		call = f
	case io.Closer:
		name = fmt.Sprintf("closer:%T", f)
		call = func(ctx context.Context) error {
			return f.Close()
		}
	default:
		return fmt.Errorf("unsupported callback type: %T", fn)
	}
	return sm.registerCall(name, call)
}

// RegisterFunc registers a simple void function.
// Convenience wrapper around Register; name is auto-generated if empty.
// Useful for quick registration of fire-and-forget cleanup.
func (sm *Shutdown) RegisterFunc(name string, fn func()) error {
	return sm.Register(fn)
}

// RegisterCall registers a simple function returning an error.
// Convenience wrapper around Register; supports jack.Func signature.
// Name is auto-generated when empty string is provided.
func (sm *Shutdown) RegisterCall(name string, fn Func) error {
	return sm.Register(fn)
}

// RegisterCloser registers an io.Closer.
// Convenience wrapper that converts Close() error into shutdown error.
// Name reflects the concrete type when left empty.
func (sm *Shutdown) RegisterCloser(name string, closer io.Closer) error {
	return sm.Register(closer)
}

// RegisterWithContext registers a fully context-aware callback.
// Allows explicit naming and direct use of jack.FuncCtx functions.
// Preferred for advanced cleanup needing cancellation/timeout awareness.
func (sm *Shutdown) RegisterWithContext(name string, fn FuncCtx) error {
	return sm.registerCall(name, fn)
}

// registerCall is the internal method that actually appends the task.
// Adds panic protection, auto-naming, and updates total event count.
// Prevents registration after shutdown has begun.
func (sm *Shutdown) registerCall(name string, fn FuncCtx) error {
	if fn == nil {
		return errors.New("callback cannot be nil")
	}
	if name == "" {
		name = runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		if name == "" {
			name = "anonymous"
		}
	}
	if sm.IsShuttingDown() {
		return errors.New("cannot register after shutdown started")
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	// Wrap user function to handle panics safely
	wrapped := func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &ShutdownError{
					Name:      name,
					Err:       fmt.Errorf("panic during shutdown: %v", r),
					Timestamp: time.Now(),
				}
				sm.log("PANIC recovered in task '%s': %v", name, r)
			}
		}()
		if callErr := fn(ctx); callErr != nil {
			return &ShutdownError{
				Name:      name,
				Err:       callErr,
				Timestamp: time.Now(),
			}
		}
		return nil
	}
	sm.events = append(sm.events, namedCall{Name: name, Fn: wrapped})
	sm.statsMu.Lock()
	sm.stats.TotalEvents++
	sm.statsMu.Unlock()
	return nil
}

// Wait blocks until a signal is received or TriggerShutdown is called.
// Returns shutdown statistics once all cleanup tasks have finished.
// This is the primary entry point for most applications.
func (sm *Shutdown) Wait() *ShutdownStats {
	if sm.IsShuttingDown() {
		<-sm.doneChan
		return sm.GetStats()
	}
	select {
	case sig := <-sm.signalChan:
		sm.log("received signal: %v", sig)
	case <-sm.forceQuit: // Will block forever if nil (feature disabled), which is correct
		sm.log("force quit triggered")
	case <-sm.shutdownCtx.Done():
		sm.log("shutdown context cancelled (timeout or manual)")
	}
	return sm.executeShutdown()
}

// WaitChan returns a channel that receives stats once shutdown is complete.
// Non-blocking alternative to Wait(), ideal for async integration.
// Channel is closed after sending the single stats value.
func (sm *Shutdown) WaitChan() <-chan *ShutdownStats {
	ch := make(chan *ShutdownStats, 1)
	go func() {
		ch <- sm.Wait()
		close(ch)
	}()
	return ch
}

// TriggerShutdown manually initiates the shutdown process.
// Useful for programmatic shutdown (e.g. health check failure).
// Returns final statistics once shutdown completes.
func (sm *Shutdown) TriggerShutdown() *ShutdownStats {
	select {
	// If we can send a signal, do so to unblock Wait()
	case sm.signalChan <- syscall.SIGTERM:
		return sm.Wait()
	default:
		// If signal channel is full or nobody is listening, execute directly
		return sm.executeShutdown()
	}
}

// executeShutdown performs the actual cleanup execution.
// Ensures idempotency, clears event list, records timing and errors.
// Called internally by Wait() and TriggerShutdown().
func (sm *Shutdown) executeShutdown() *ShutdownStats {
	// Ensure only one execution runs
	if !sm.inShutdown.CompareAndSwap(false, true) {
		<-sm.doneChan
		return sm.GetStats()
	}
	sm.mu.Lock()
	events := sm.events
	sm.events = nil // Clear to prevent double execution
	sm.mu.Unlock()
	sm.statsMu.Lock()
	sm.stats.StartTime = time.Now()
	sm.stats.TotalEvents = len(events)
	sm.statsMu.Unlock()
	sm.log("starting shutdown of %d task(s)", len(events))
	// Crucially, use sm.shutdownCtx as the parent.
	// This ensures that if ForceQuitMonitor calls sm.cancelFunc(),
	// this context (and the tasks using it) are cancelled immediately.
	var cleanupCtx context.Context
	var cleanupCancel context.CancelFunc
	if sm.timeout > 0 {
		// Apply the timeout NOW, relative to when shutdown started
		cleanupCtx, cleanupCancel = context.WithTimeout(sm.shutdownCtx, sm.timeout)
	} else {
		cleanupCtx, cleanupCancel = context.WithCancel(sm.shutdownCtx)
	}
	defer cleanupCancel()
	if len(events) > 0 {
		if sm.concurrent {
			sm.executeConcurrent(events, cleanupCtx) // Pass cleanupCtx
		} else {
			sm.executeSequential(events, cleanupCtx) // Pass cleanupCtx
		}
	}
	sm.statsMu.Lock()
	sm.stats.EndTime = time.Now()
	sm.statsMu.Unlock()
	duration := sm.stats.EndTime.Sub(sm.stats.StartTime)
	sm.log("shutdown completed in %v (failed: %d)", duration, sm.stats.FailedEvents)
	sm.cancelFunc()
	close(sm.doneChan)
	signal.Stop(sm.signalChan)
	return sm.GetStats()
}

// executeSequential runs cleanup tasks in LIFO order (last registered first).
// Blocks until all tasks complete or context is cancelled.
// Updates completion/failure counters for each task.
func (sm *Shutdown) executeSequential(events []namedCall, ctx context.Context) {
	// LIFO (Last-In-First-Out) execution
	for i := len(events) - 1; i >= 0; i-- {
		nc := events[i]
		sm.log("running: %s", nc.Name)
		if err := nc.Fn(ctx); err != nil {
			sm.log("task failed: %s -> %v", nc.Name, err)
			sm.recordError(err)
			sm.statsMu.Lock()
			sm.stats.FailedEvents++
			sm.statsMu.Unlock()
		} else {
			sm.statsMu.Lock()
			sm.stats.CompletedEvents++
			sm.statsMu.Unlock()
		}
	}
}

// executeConcurrent runs all cleanup tasks in parallel using goroutines.
// Collects errors via channel and waits for completion with WaitGroup.
// Significantly faster for independent cleanup operations.
func (sm *Shutdown) executeConcurrent(events []namedCall, ctx context.Context) {
	var wg sync.WaitGroup
	errChan := make(chan error, len(events))
	for _, nc := range events {
		wg.Add(1)
		go func(task namedCall) {
			defer wg.Done()
			sm.log("running (concurrent): %s", task.Name)
			errChan <- task.Fn(ctx)
		}(nc)
	}
	go func() {
		wg.Wait()
		close(errChan)
	}()
	failed := 0
	for err := range errChan {
		if err != nil {
			failed++
			sm.recordError(err)
			// Try to log the name if it's our wrapped error type
			var se *ShutdownError
			if errors.As(err, &se) {
				sm.log("task failed (concurrent): %s -> %v", se.Name, se.Err)
			}
		}
	}
	sm.statsMu.Lock()
	sm.stats.FailedEvents += failed
	sm.stats.CompletedEvents += len(events) - failed
	sm.statsMu.Unlock()
}

// log writes formatted message using the configured logger.
// No-op when logger is nil (safe default behavior).
// Uses Infof level for all shutdown-related messages.
func (sm *Shutdown) log(format string, v ...any) {
	if sm.logger != nil {
		sm.logger.Infof(format, v...)
	}
}

// IsShuttingDown reports whether shutdown has been initiated.
// Thread-safe via atomic boolean; useful for guarding late registrations.
// Returns true once the first shutdown trigger occurs.
func (sm *Shutdown) IsShuttingDown() bool {
	return sm.inShutdown.Load()
}

// recordError appends a failed task error to the stats structure.
// Thread-safe; used by both sequential and concurrent executors.
// Preserves full error chain including wrapped ShutdownError.
func (sm *Shutdown) recordError(err error) {
	sm.statsMu.Lock()
	defer sm.statsMu.Unlock()
	sm.stats.Errors = append(sm.stats.Errors, err)
}

// GetStats returns a deep copy of current shutdown statistics.
// Safe for concurrent reads; protects against mutation during access.
// Used by public APIs to return final results.
// Used by public APIs to return final results.
func (sm *Shutdown) GetStats() *ShutdownStats {
	sm.statsMu.RLock()
	defer sm.statsMu.RUnlock()
	copyx := *sm.stats
	if sm.stats.Errors != nil {
		copyx.Errors = make([]error, len(sm.stats.Errors))
		copy(copyx.Errors, sm.stats.Errors)
	}
	return &copyx
}

// Done returns a channel that's closed when shutdown completes.
// Allows integration with context or select-based waiting patterns.
// Compatible with context.Done() style usage.
func (sm *Shutdown) Done() <-chan struct{} {
	return sm.doneChan
}

// ShutdownError provides structured error details.
type ShutdownError struct {
	Name      string
	Err       error
	Timestamp time.Time
}

func (e *ShutdownError) Error() string {
	return fmt.Sprintf("%s: %v", e.Name, e.Err)
}

func (e *ShutdownError) Unwrap() error {
	return e.Err
}

// autoName reflects the function name.
// Falls back to ULID-based unique name if reflection fails.
// Guarantees every task has a readable identifier in logs/stats.
func autoName(fn any) string {
	val := reflect.ValueOf(fn)
	if val.Kind() == reflect.Func {
		name := runtime.FuncForPC(val.Pointer()).Name()
		if name != "" {
			return name
		}
	}
	return fmt.Sprintf("anon-%s", ulid.Make().String())
}

package jack

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/olekukonko/ll"
)

// Schedule represents an event emitted by the scheduler for observability.
// It encapsulates metadata about scheduling events, such as task submission or termination, to facilitate monitoring and debugging.
type Schedule struct {
	Type     string    // Type of event (e.g., "task_submitted", "task_submission_failed", "stopped")
	Name     string    // Name of the scheduler emitting the event
	TaskID   string    // Unique identifier for the task
	TaskType string    // Type of the task (e.g., struct name or type description)
	Message  string    // Descriptive message providing context for the event
	Error    error     // Any error associated with the event, if applicable
	Routine  Routine   // Configuration of the scheduling routine (e.g., interval, max runs)
	Time     time.Time // Timestamp when the event occurred
	NextRun  time.Time // Scheduled time for the next task execution, if applicable
}

// Cycle is a functional option type for configuring a Scheduler.
// It uses the functional options pattern to allow flexible and extensible configuration of scheduler settings.
type Cycle func(*Scheduling)

// Scheduling holds configuration for retry behavior and observability.
// It defines how the scheduler handles task submission retries and event notifications.
type Scheduling struct {
	observable   Observable[Schedule] // Observable interface for emitting scheduler events to external systems
	RetryCount   int                  // Number of retry attempts for task submission on failure
	RetryBackoff time.Duration        // Duration to wait between retry attempts for failed submissions
}

// SchedulingWithObservable returns a Cycle to set an observable for events.
// This allows external systems (e.g., logging or monitoring tools) to receive and process scheduler events.
func SchedulingWithObservable(obs Observable[Schedule]) Cycle {
	return func(cfg *Scheduling) {
		cfg.observable = obs // Set the observable for event notifications
	}
}

// SchedulingWithRetry returns a Cycle to configure retry attempts on queue full errors.
// It allows customization of retry count and backoff duration for handling task submission failures.
func SchedulingWithRetry(count int, backoff time.Duration) Cycle {
	return func(opts *Scheduling) {
		if count > 0 {
			opts.RetryCount = count // Set retry count if positive
		}
		if backoff > 0 {
			opts.RetryBackoff = backoff // Set backoff duration if positive
		}
	}
}

// Scheduler manages periodic or limited task submissions to a Pool.
// It orchestrates task execution based on a routine (interval-based or limited runs), handles retries, and ensures thread-safe operations.
type Scheduler struct {
	name           string             // Unique identifier for the scheduler
	pool           *Pool              // Task execution pool where tasks are submitted
	routine        Routine            // Scheduling routine defining interval or max runs
	cfg            Scheduling         // Configuration for retries and observability
	mu             sync.Mutex         // Mutex for thread-safe access to scheduler state
	running        bool               // Flag indicating if the scheduler is currently active
	activeTasks    []interface{}      // List of tasks currently being scheduled
	taskRunCtx     context.Context    // Context for task execution, used for context-aware tasks
	runnerCancelFn context.CancelFunc // Function to cancel the scheduler's loop context
	runnerWg       sync.WaitGroup     // WaitGroup to track active task scheduling goroutines
	logger         *ll.Logger         // Logger instance for logging scheduler events
}

// NewScheduler creates a new Scheduler instance.
// It initializes the scheduler with a name, task pool, routine, and optional configurations, ensuring valid inputs.
func NewScheduler(name string, pool *Pool, schedule Routine, opts ...Cycle) (*Scheduler, error) {
	// Validate input parameters
	if name == "" {
		return nil, ErrSchedulerNameMissing // Return error if scheduler name is empty
	}
	if pool == nil {
		return nil, ErrSchedulerPoolNil // Return error if task pool is nil
	}

	// Initialize default configuration for retries
	config := Scheduling{
		RetryCount:   retryScheduler,        // Default to 3 retry attempts for failed submissions
		RetryBackoff: retrySchedulerBackoff, // Default backoff of 100ms between retries
	}
	// Apply provided functional options to customize configuration
	for _, opt := range opts {
		opt(&config)
	}

	// Set up logger with scheduler-specific namespace for contextual logging
	lo := logger.Namespace("scheduler")
	pool.Logger(lo) // Configure pool to use the same logger

	// Return initialized scheduler instance
	return &Scheduler{
		name:    name,
		pool:    pool,
		routine: schedule,
		cfg:     config,
		logger:  lo,
	}, nil
}

// loop executes the scheduling logic for a single task.
// It handles both immediate and interval-based task submissions, tracks run counts, and respects cancellation signals.
// The method runs in its own goroutine and emits observability events for key actions (e.g., submission, failure, stopping).
func (s *Scheduler) loop(runnerCtx context.Context, taskToRun interface{}, perExecutionCtx context.Context) {
	defer s.runnerWg.Done()                    // Decrement WaitGroup when loop exits
	taskTypeName := typeName(taskToRun)        // Get task type for logging and events
	taskRefID := defaultIDScheduler(taskToRun) // Get unique task ID

	// Handle immediate first run if interval is set
	if s.routine.Interval > 0 {
		if _, submitted := s.submit(taskToRun, perExecutionCtx); submitted {
			s.emit("task_submitted", taskRefID, taskTypeName, time.Now(), "First task submitted immediately", nil)
		} else {
			s.emit("task_submission_failed", taskRefID, taskTypeName, time.Now(), "Failed to submit first immediate task", nil)
		}
	}

	runsCounter := 1 // Account for immediate run
	if s.routine.Interval <= 0 {
		runsCounter = 0 // No immediate run for non-interval tasks
	}

	// Handle non-interval-based execution (run a fixed number of times)
	if s.routine.Interval <= 0 {
		maxRuns := s.routine.MaxRuns
		if maxRuns == 0 {
			maxRuns = 1 // Default to one run if MaxRuns is unset
		}
		for i := 0; i < maxRuns; i++ {
			select {
			case <-runnerCtx.Done():
				return // Exit if scheduler is canceled
			default:
				s.submit(taskToRun, perExecutionCtx) // Attempt task submission
			}
		}
		s.emit("run_limit_reached", taskRefID, taskTypeName, time.Now(), fmt.Sprintf("Max %d non-interval runs completed", maxRuns), nil)
		return
	}

	// Handle interval-based execution (run periodically)
	ticker := time.NewTicker(s.routine.Interval) // Create ticker for periodic execution
	defer ticker.Stop()                          // Ensure ticker is stopped when loop exits

	for {
		if s.routine.MaxRuns > 0 && runsCounter >= s.routine.MaxRuns {
			s.emit("run_limit_reached", taskRefID, taskTypeName, time.Now(), fmt.Sprintf("Max %d interval runs completed", s.routine.MaxRuns), nil)
			return // Exit if max runs reached
		}

		select {
		case tickTime := <-ticker.C:
			if _, submitted := s.submit(taskToRun, perExecutionCtx); submitted {
				s.emit("task_submitted", taskRefID, taskTypeName, tickTime, "Task submitted on tick", nil)
				runsCounter++ // Increment run counter on successful submission
			} else {
				s.emit("task_submission_failed", taskRefID, taskTypeName, tickTime, "Failed to submit task on tick", nil)
			}
		case <-runnerCtx.Done():
			s.emit("stopped", taskRefID, taskTypeName, time.Now(), "Scheduler stopped", runnerCtx.Err())
			return // Exit if scheduler is canceled
		}
	}
}

// Do starts scheduling for the provided non-context-aware tasks.
// It initializes the scheduler to execute the given tasks according to the configured routine (e.g., interval or max runs).
// The method ensures thread-safety by locking the scheduler state and checks if the scheduler is already running to prevent duplicate executions.
// Each task is executed in its own goroutine, and the method emits a "started" event for observability.
func (s *Scheduler) Do(ts ...Task) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return ErrSchedulerJobAlreadyRunning // Prevent starting if already running
	}

	// Initialize active tasks list with provided tasks
	s.activeTasks = make([]interface{}, len(ts))
	for i, t := range ts {
		s.activeTasks[i] = t
	}

	// Create a context for controlling the scheduler loop lifecycle
	runnerCtx, cancel := context.WithCancel(context.Background())
	s.runnerCancelFn = cancel
	s.running = true // Mark scheduler as running
	s.mu.Unlock()

	// Increment WaitGroup for each task to track active goroutines
	s.runnerWg.Add(len(s.activeTasks))
	for _, task := range s.activeTasks {
		// Emit observability event for task start
		s.emit("started", defaultIDScheduler(task), typeName(task), time.Now(), "Scheduler job started", nil)
		// Start a goroutine to handle task scheduling loop
		go s.loop(runnerCtx, task, nil)
	}

	return nil // Successful start
}

// DoCtx starts scheduling for context-aware tasks.
// Similar to Do, but designed for tasks that accept a context for cancellation or deadlines.
// It ensures thread-safety, checks for existing runs, and associates a task execution context with the tasks.
// Each task runs in its own goroutine, and a "started" event is emitted for observability.
func (s *Scheduler) DoCtx(taskExecCtx context.Context, ts ...TaskCtx) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return ErrSchedulerJobAlreadyRunning // Prevent starting if already running
	}

	// Use background context if none provided
	if taskExecCtx == nil {
		taskExecCtx = context.Background()
	}

	// Initialize active tasks list with provided tasks
	s.activeTasks = make([]interface{}, len(ts))
	for i, t := range ts {
		s.activeTasks[i] = t
	}
	s.taskRunCtx = taskExecCtx // Store task execution context

	// Create a context for controlling the scheduler loop lifecycle
	runnerCtx, cancel := context.WithCancel(context.Background())
	s.runnerCancelFn = cancel
	s.running = true // Mark scheduler as running
	s.mu.Unlock()

	// Increment WaitGroup for each task to track active goroutines
	s.runnerWg.Add(len(s.activeTasks))
	for _, task := range s.activeTasks {
		// Emit observability event for task start
		s.emit("started", defaultIDScheduler(task), typeName(task), time.Now(), "Scheduler job started", nil)
		// Start a goroutine to handle task scheduling loop with execution context
		go s.loop(runnerCtx, task, s.taskRunCtx)
	}

	return nil // Successful start
}

// Terminate gracefully stops all running scheduler loops.
// It cancels the runner context, waits for all task loops to complete, and optionally shuts down the task pool.
// The method is thread-safe and emits "stopped" events for each task upon termination.
func (s *Scheduler) Terminate(cancelPool bool) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrSchedulerNotRunning // Return error if scheduler is not active
	}

	// Cancel the scheduler loop context if it exists
	if s.runnerCancelFn != nil {
		s.runnerCancelFn()
	}
	tasksToStop := s.activeTasks // Capture tasks to emit stop events
	s.mu.Unlock()

	// Wait for all task loops to complete
	s.runnerWg.Wait()

	s.mu.Lock()
	s.running = false   // Mark scheduler as stopped
	s.activeTasks = nil // Clear active tasks
	s.mu.Unlock()

	// Optionally shut down the task pool
	if cancelPool {
		s.pool.Shutdown(time.Second * 5) // Allow 5 seconds for pool shutdown
	}

	// Emit stop events for each task
	for _, task := range tasksToStop {
		s.emit("stopped", defaultIDScheduler(task), typeName(task), time.Now(), "Scheduler job explicitly stopped.", nil)
	}
	return nil
}

// Stop terminates the scheduler without shutting down the pool.
// It calls Terminate with cancelPool set to false, providing a convenient way to stop task scheduling while keeping the pool active.
func (s *Scheduler) Stop() error {
	return s.Terminate(false) // Gracefully stop scheduler without pool shutdown
}

// submit attempts to send a task to the pool.
// It handles both non-context-aware (Task) and context-aware (TaskCtx) tasks, with retry logic for queue full errors.
// The method logs failures and recovers from panics to ensure robustness.
func (s *Scheduler) submit(taskToRun interface{}, perExecutionCtx context.Context) (string, bool) {
	taskReferenceID := defaultIDScheduler(taskToRun) // Get task ID
	taskTypeName := typeName(taskToRun)              // Get task type name

	// Check if pool is valid
	if s.pool == nil {
		s.logger.Info("Scheduler [%s]: Pool is nil.", s.name)
		return taskReferenceID, false // Return failure if pool is nil
	}

	var err error
	// Recover from panics during submission to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during submission: %v, stack: %s", r, string(debug.Stack()))
		}
		if err != nil {
			s.logger.Info("Scheduler [%s]: Failed to submit task %s: %v", s.name, taskTypeName, err)
		}
	}()

	// Handle task submission based on task type
	switch task := taskToRun.(type) {
	case Task:
		err = s.pool.Submit(task) // Submit non-context-aware task
		if errors.Is(err, ErrQueueFull) && s.cfg.RetryCount > 0 {
			// Retry submission on queue full error
			for i := 0; i < s.cfg.RetryCount; i++ {
				time.Sleep(s.cfg.RetryBackoff) // Wait before retrying
				if s.pool.Submit(task) == nil {
					err = nil // Success on retry
					break
				}
			}
		}
	case TaskCtx:
		execCtx := perExecutionCtx
		if execCtx == nil {
			execCtx = context.Background() // Use background context if none provided
		}
		if err = execCtx.Err(); err != nil {
			return taskReferenceID, false // Return failure if context is canceled
		}
		err = s.pool.SubmitCtx(execCtx, task) // Submit context-aware task
	default:
		err = fmt.Errorf("unknown task type: %T", taskToRun) // Handle unsupported task types
	}
	return taskReferenceID, err == nil // Return task ID and submission success status
}

// Name returns the scheduler's identifying name.
// It provides a thread-safe way to access the scheduler's name for identification purposes.
func (s *Scheduler) Name() string {
	return s.name
}

// Running checks if the scheduler is currently active.
// It performs a thread-safe check of the running flag to determine if the scheduler is processing tasks.
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// emit sends a Schedule event to the configured observable.
// It constructs and dispatches an event with details about the scheduler's state or actions for monitoring purposes.
func (s *Scheduler) emit(eventType string, taskRefID string, taskTypeName string, eventTime time.Time, message string, err error) {
	if s.cfg.observable == nil {
		return // Skip if no observable is configured
	}
	if eventTime.IsZero() {
		eventTime = time.Now() // Default to current time if none provided
	}
	// Construct event with relevant details
	event := Schedule{
		Type:     eventType,
		Name:     s.name,
		Time:     eventTime,
		TaskID:   taskRefID,
		TaskType: taskTypeName,
		Routine:  s.routine,
		Message:  message,
		Error:    err,
	}
	s.cfg.observable.Notify(event) // Notify observers of the event
}

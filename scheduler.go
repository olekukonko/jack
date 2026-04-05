package jack

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/olekukonko/ll"
	"github.com/robfig/cron/v3"
)

// Schedule represents an event emitted by the scheduler for observability.
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
type Scheduling struct {
	observable   Observable[Schedule] // Observable interface for emitting scheduler events
	RetryCount   int                  // Number of retry attempts for task submission on failure
	RetryBackoff time.Duration        // Duration to wait between retry attempts for failed submissions
}

// SchedulingWithObservable returns a Cycle to set an observable for events.
func SchedulingWithObservable(obs Observable[Schedule]) Cycle {
	return func(cfg *Scheduling) {
		cfg.observable = obs
	}
}

// SchedulingWithRetry returns a Cycle to configure retry attempts on queue full errors.
func SchedulingWithRetry(count int, backoff time.Duration) Cycle {
	return func(opts *Scheduling) {
		if count > 0 {
			opts.RetryCount = count
		}
		if backoff > 0 {
			opts.RetryBackoff = backoff
		}
	}
}

// cronJob wraps a jack Task or TaskCtx to implement cron.Job interface.
type cronJob struct {
	task      interface{}
	pool      *Pool
	ctx       context.Context
	scheduler *Scheduler
	taskID    string
	taskType  string
}

// Run implements the cron.Job interface.
func (c *cronJob) Run() {
	if c.ctx == nil {
		c.ctx = context.Background()
	}
	c.scheduler.submit(c.task, c.ctx)
	c.scheduler.emit("task_submitted", c.taskID, c.taskType, time.Now(), "Task submitted via cron schedule", nil)
}

// Scheduler manages periodic or limited task submissions to a Pool.
// It supports both interval-based and cron expression-based scheduling.
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
	cron           *cron.Cron         // Cron scheduler for cron-based tasks
	cronIDs        []cron.EntryID
}

// NewScheduler creates a new Scheduler instance.
func NewScheduler(name string, pool *Pool, schedule Routine, opts ...Cycle) (*Scheduler, error) {
	if name == "" {
		return nil, ErrSchedulerNameMissing
	}
	if pool == nil {
		return nil, ErrSchedulerPoolNil
	}
	config := Scheduling{
		RetryCount:   retryScheduler,
		RetryBackoff: retrySchedulerBackoff,
	}
	for _, opt := range opts {
		opt(&config)
	}
	lo := logger.Namespace("scheduler")
	pool.Logger(lo)
	return &Scheduler{
		name:    name,
		pool:    pool,
		routine: schedule,
		cfg:     config,
		logger:  lo,
		cronIDs: make([]cron.EntryID, 0),
	}, nil
}

// loop executes the scheduling logic for a single task.
// It handles both immediate and interval-based task submissions, tracks run counts, and respects cancellation signals.
// The method runs in its own goroutine and emits observability events for key actions (e.g., submission, failure, stopping).
func (s *Scheduler) loop(runnerCtx context.Context, taskToRun interface{}, perExecutionCtx context.Context) {
	defer s.runnerWg.Done()
	taskTypeName := typeName(taskToRun)
	taskRefID := defaultIDScheduler(taskToRun)

	if s.routine.Interval > 0 {
		if _, submitted := s.submit(taskToRun, perExecutionCtx); submitted {
			s.emit("task_submitted", taskRefID, taskTypeName, time.Now(), "First task submitted immediately", nil)
		} else {
			s.emit("task_submission_failed", taskRefID, taskTypeName, time.Now(), "Failed to submit first immediate task", nil)
		}
	}
	runsCounter := 1
	if s.routine.Interval <= 0 {
		runsCounter = 0
	}

	if s.routine.Interval <= 0 {
		maxRuns := s.routine.MaxRuns
		if maxRuns == 0 {
			maxRuns = 1
		}
		for i := 0; i < maxRuns; i++ {
			select {
			case <-runnerCtx.Done():
				return
			default:
				s.submit(taskToRun, perExecutionCtx)
			}
		}
		s.emit("run_limit_reached", taskRefID, taskTypeName, time.Now(), fmt.Sprintf("Max %d non-interval runs completed", maxRuns), nil)
		return
	}

	ticker := time.NewTicker(s.routine.Interval)
	defer ticker.Stop()
	for {
		if s.routine.MaxRuns > 0 && runsCounter >= s.routine.MaxRuns {
			s.emit("run_limit_reached", taskRefID, taskTypeName, time.Now(), fmt.Sprintf("Max %d interval runs completed", s.routine.MaxRuns), nil)
			return
		}
		select {
		case tickTime := <-ticker.C:
			if _, submitted := s.submit(taskToRun, perExecutionCtx); submitted {
				s.emit("task_submitted", taskRefID, taskTypeName, tickTime, "Task submitted on tick", nil)
				runsCounter++
			} else {
				s.emit("task_submission_failed", taskRefID, taskTypeName, tickTime, "Failed to submit task on tick", nil)
			}
		case <-runnerCtx.Done():
			s.emit("stopped", taskRefID, taskTypeName, time.Now(), "Scheduler stopped", runnerCtx.Err())
			return
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
		return ErrSchedulerJobAlreadyRunning
	}

	// Check if cron-based scheduling should be used
	if s.routine.Cron != "" {
		// Convert []Task to []interface{}
		tasks := make([]interface{}, len(ts))
		for i, t := range ts {
			tasks[i] = t
		}
		return s.startCronLocked(tasks, nil)
	}

	s.activeTasks = make([]interface{}, len(ts))
	for i, t := range ts {
		s.activeTasks[i] = t
	}
	runnerCtx, cancel := context.WithCancel(context.Background())
	s.runnerCancelFn = cancel
	s.running = true
	s.mu.Unlock()

	s.runnerWg.Add(len(s.activeTasks))
	for _, task := range s.activeTasks {
		s.emit("started", defaultIDScheduler(task), typeName(task), time.Now(), "Scheduler job started", nil)
		go s.loop(runnerCtx, task, nil)
	}
	return nil
}

// DoCtx starts scheduling for context-aware tasks.
// Similar to Do, but designed for tasks that accept a context for cancellation or deadlines.
// It ensures thread-safety, checks for existing runs, and associates a task execution context with the tasks.
// Each task runs in its own goroutine, and a "started" event is emitted for observability.
func (s *Scheduler) DoCtx(taskExecCtx context.Context, ts ...TaskCtx) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return ErrSchedulerJobAlreadyRunning
	}

	// Check if cron-based scheduling should be used
	if s.routine.Cron != "" {
		// Convert []TaskCtx to []interface{}
		tasks := make([]interface{}, len(ts))
		for i, t := range ts {
			tasks[i] = t
		}
		return s.startCronLocked(tasks, taskExecCtx)
	}

	if taskExecCtx == nil {
		taskExecCtx = context.Background()
	}
	s.activeTasks = make([]interface{}, len(ts))
	for i, t := range ts {
		s.activeTasks[i] = t
	}
	s.taskRunCtx = taskExecCtx
	runnerCtx, cancel := context.WithCancel(context.Background())
	s.runnerCancelFn = cancel
	s.running = true
	s.mu.Unlock()

	s.runnerWg.Add(len(s.activeTasks))
	for _, task := range s.activeTasks {
		s.emit("started", defaultIDScheduler(task), typeName(task), time.Now(), "Scheduler job started", nil)
		go s.loop(runnerCtx, task, s.taskRunCtx)
	}
	return nil
}

// startCronLocked starts cron-based scheduling while holding the lock.
func (s *Scheduler) startCronLocked(tasks []interface{}, execCtx context.Context) error {
	if execCtx == nil {
		execCtx = context.Background()
	}

	// Create cron with seconds support and panic recovery
	s.cron = cron.New(
		cron.WithSeconds(),
		cron.WithChain(
			cron.Recover(cron.PrintfLogger(s.logger)),
		),
	)

	for _, t := range tasks {
		taskTypeName := typeName(t)
		taskRefID := defaultIDScheduler(t)

		job := &cronJob{
			task:      t,
			pool:      s.pool,
			ctx:       execCtx,
			scheduler: s,
			taskID:    taskRefID,
			taskType:  taskTypeName,
		}

		entryID, err := s.cron.AddJob(s.routine.Cron, job)
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("failed to add cron job for %s: %w", taskRefID, err)
		}
		s.cronIDs = append(s.cronIDs, entryID)
		s.emit("started", taskRefID, taskTypeName, time.Now(), "Cron scheduler job started", nil)
	}

	s.activeTasks = tasks
	s.running = true
	s.mu.Unlock()

	s.cron.Start()
	return nil
}

// Terminate gracefully stops all running scheduler loops.
func (s *Scheduler) Terminate(cancelPool bool) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrSchedulerNotRunning
	}

	// Stop cron scheduler if active
	if s.cron != nil {
		ctx := s.cron.Stop()
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		s.cron = nil
		s.cronIDs = nil
	}

	if s.runnerCancelFn != nil {
		s.runnerCancelFn()
	}
	tasksToStop := s.activeTasks
	s.mu.Unlock()

	s.runnerWg.Wait()

	s.mu.Lock()
	s.running = false
	s.activeTasks = nil
	s.mu.Unlock()

	if cancelPool {
		s.pool.Shutdown(time.Second * 5)
	}

	for _, task := range tasksToStop {
		s.emit("stopped", defaultIDScheduler(task), typeName(task), time.Now(), "Scheduler job explicitly stopped.", nil)
	}
	return nil
}

// Stop terminates the scheduler without shutting down the pool.
func (s *Scheduler) Stop() error {
	return s.Terminate(false)
}

// submit attempts to send a task to the pool.
func (s *Scheduler) submit(taskToRun interface{}, perExecutionCtx context.Context) (string, bool) {
	taskReferenceID := defaultIDScheduler(taskToRun)
	taskTypeName := typeName(taskToRun)

	if s.pool == nil {
		s.logger.Info("Scheduler [%s]: Pool is nil.", s.name)
		return taskReferenceID, false
	}

	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during submission: %v, stack: %s", r, string(debug.Stack()))
		}
		if err != nil {
			s.logger.Info("Scheduler [%s]: Failed to submit task %s: %v", s.name, taskTypeName, err)
		}
	}()

	switch task := taskToRun.(type) {
	case Task:
		err = s.pool.Submit(task)
		if errors.Is(err, ErrQueueFull) && s.cfg.RetryCount > 0 {
			for i := 0; i < s.cfg.RetryCount; i++ {
				time.Sleep(s.cfg.RetryBackoff)
				if s.pool.Submit(task) == nil {
					err = nil
					break
				}
			}
		}
	case TaskCtx:
		execCtx := perExecutionCtx
		if execCtx == nil {
			execCtx = context.Background()
		}
		if err = execCtx.Err(); err != nil {
			return taskReferenceID, false
		}
		err = s.pool.SubmitCtx(execCtx, task)
	default:
		err = fmt.Errorf("unknown task type: %T", taskToRun)
	}
	return taskReferenceID, err == nil
}

// Name returns the scheduler's identifying name.
func (s *Scheduler) Name() string {
	return s.name
}

// Running checks if the scheduler is currently active.
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// emit sends a Schedule event to the configured observable.
func (s *Scheduler) emit(eventType string, taskRefID string, taskTypeName string, eventTime time.Time, message string, err error) {
	if s.cfg.observable == nil {
		return
	}
	if eventTime.IsZero() {
		eventTime = time.Now()
	}
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
	s.cfg.observable.Notify(event)
}

// Entries returns the current cron entries if using cron-based scheduling.
func (s *Scheduler) Entries() []cron.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cron == nil {
		return nil
	}
	return s.cron.Entries()
}

// NextRun returns the next scheduled run time for the first task.
func (s *Scheduler) NextRun() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cron != nil {
		entries := s.cron.Entries()
		if len(entries) > 0 {
			return entries[0].Next, true
		}
	}
	return time.Time{}, false
}

// Package jack manages a worker pool for concurrent task execution with logging and observability.
package jack

import (
	"context"
	"errors"
	"fmt"
	"github.com/olekukonko/ll"
	"runtime/debug"
	"sync"
	"time"
)

// Schedule captures details of a scheduled task event for observability.
// Thread-safe for use in notification across goroutines.
type Schedule struct {
	Type     string    // Event type (e.g., "started", "task_submitted", "stopped")
	Name     string    // Scheduler name
	TaskID   string    // Task identifier
	TaskType string    // Type of the task
	Message  string    // Descriptive message
	Error    error     // Error, if any
	Routine  Routine   // Routine configuration
	Time     time.Time // Time of the event
	NextRun  time.Time // Scheduled time for the next run
}

// Cycle configures a Scheduling instance during creation.
type Cycle func(*Scheduling)

// Scheduling holds configuration for a Scheduler, including its observable for events.
// Thread-safe via pointer updates and observable notifications.
type Scheduling struct {
	observable Observable[Schedule] // Observable for schedule events
}

// Observable sets the observable for the Scheduling configuration.
// Thread-safe as it updates the observable pointer.
// Example:
//
//	s.Observable(obs) // Sets the observable for scheduling events
func (s *Scheduling) Observable(obs Observable[Schedule]) {
	s.observable = obs
}

// SchedulingWithObservable returns a Cycle that sets the observable for Scheduling.
// Example:
//
//	scheduler, _ := NewScheduler("name", pool, routine, SchedulingWithObservable(obs)) // Configures observable
func SchedulingWithObservable(obs Observable[Schedule]) Cycle {
	return func(cfg *Scheduling) {
		cfg.observable = obs
	}
}

// Scheduler manages recurring or one-time task execution using a worker pool.
// It supports task submission, logging, and observability of scheduling events.
// Thread-safe via mutex and wait group synchronization.
type Scheduler struct {
	name           string             // Unique name of the scheduler
	pool           *Pool              // Worker pool for task execution
	routine        Routine            // Routine configuration for scheduling
	cfg            Scheduling         // Scheduler configuration
	mu             sync.Mutex         // Protects running state and task submission
	running        bool               // Indicates if the scheduler is active
	task           interface{}        // Current task (Task or TaskCtx)
	taskRunCtx     context.Context    // Context for TaskCtx execution
	runnerCancelFn context.CancelFunc // Cancels the runner loop
	runnerWg       sync.WaitGroup     // Waits for the runner goroutine
	logger         *ll.Logger         // Logger with scheduler namespace
}

// NewScheduler creates a new Scheduler with the given name, pool, routine, and options.
// It returns an error if the name is empty or the pool is nil.
// Thread-safe via initialization and logger namespace.
// Example:
//
//	scheduler, err := NewScheduler("myScheduler", pool, Routine{Interval: time.Second}, SchedulingWithObservable(obs))
func NewScheduler(name string, pool *Pool, schedule Routine, opts ...Cycle) (*Scheduler, error) {
	if name == "" {
		return nil, ErrSchedulerNameMissing
	}
	if pool == nil {
		return nil, ErrSchedulerPoolNil
	}

	config := Scheduling{}
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
	}, nil
}

// emit sends a scheduling event to the observable with the specified details.
// It uses the current time if eventTime is zero.
// Thread-safe via observable notifications.
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

// loop runs the scheduler’s task execution loop in a goroutine.
// It submits tasks based on the routine configuration and handles cancellation.
// Thread-safe via context and wait group.
func (s *Scheduler) loop(runnerCtx context.Context, taskToRun interface{}, perExecutionCtx context.Context) {
	defer s.runnerWg.Done()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()
	taskTypeName := typeName(taskToRun)

	firstRunDone := false
	if s.routine.Interval > 0 {
		select {
		case <-runnerCtx.Done():
			s.emit("stopped", defaultIDScheduler(taskToRun), taskTypeName, time.Now(), "Scheduler stopped before first immediate run", runnerCtx.Err())
			return
		default:
			taskRefID, submitted := s.submit(taskToRun, perExecutionCtx)
			if submitted {
				s.emit("task_submitted", taskRefID, taskTypeName, time.Now(), "First task submitted immediately", nil)
			} else {
				s.emit("task_submission_failed", taskRefID, taskTypeName, time.Now(), "Failed to submit first immediate task", nil)
				if _, isTaskCtx := taskToRun.(TaskCtx); isTaskCtx && perExecutionCtx.Err() != nil {
					return
				}
			}
			firstRunDone = true
		}
	}

	runsCounter := 0
	if firstRunDone {
		runsCounter = 1
	}

	if s.routine.Interval <= 0 {
		maxRuns := s.routine.MaxRuns
		if maxRuns == 0 {
			maxRuns = 1
		}
		startIndex := 0
		if firstRunDone {
			startIndex = 1
		}

		for i := startIndex; i < maxRuns; i++ {
			select {
			case <-runnerCtx.Done():
				s.emit("stopped", defaultIDScheduler(taskToRun), taskTypeName, time.Now(), "Scheduler stopped during non-interval runs", runnerCtx.Err())
				return
			default:
				taskRefID, submitted := s.submit(taskToRun, perExecutionCtx)
				msg := fmt.Sprintf("Non-interval run attempt %d/%d", i+1, maxRuns)
				if submitted {
					s.emit("task_submitted", taskRefID, taskTypeName, time.Now(), msg, nil)
				} else {
					s.emit("task_submission_failed", taskRefID, taskTypeName, time.Now(), msg, nil)
					if _, isTaskCtx := taskToRun.(TaskCtx); isTaskCtx && perExecutionCtx.Err() != nil {
						return
					}
				}
			}
		}
		if maxRuns > 0 {
			s.emit("run_limit_reached", defaultIDScheduler(taskToRun), taskTypeName, time.Now(), fmt.Sprintf("Max %d non-interval runs completed", maxRuns), nil)
		}
		return
	}

	ticker := time.NewTicker(s.routine.Interval)
	defer ticker.Stop()

	for {
		if s.routine.MaxRuns > 0 && runsCounter >= s.routine.MaxRuns {
			s.emit("run_limit_reached", defaultIDScheduler(taskToRun), taskTypeName, time.Now(), fmt.Sprintf("Max %d interval runs completed", s.routine.MaxRuns), nil)
			return
		}

		select {
		case tickTime := <-ticker.C:
			select {
			case <-runnerCtx.Done():
				s.emit("stopped", defaultIDScheduler(taskToRun), taskTypeName, time.Now(), "Scheduler stopped while waiting for tick", runnerCtx.Err())
				return
			default:
			}

			s.emit("tick_triggered", defaultIDScheduler(taskToRun), taskTypeName, tickTime, "Scheduler tick triggered", nil)
			taskRefID, submitted := s.submit(taskToRun, perExecutionCtx)
			if submitted {
				s.emit("task_submitted", taskRefID, taskTypeName, time.Now(), "Task submitted on tick", nil)
			} else {
				s.emit("task_submission_failed", taskRefID, taskTypeName, time.Now(), "Failed to submit task on tick", nil)
				if _, isTaskCtx := taskToRun.(TaskCtx); isTaskCtx && perExecutionCtx.Err() != nil {
					return
				}
			}
			runsCounter++

		case <-runnerCtx.Done():
			s.emit("stopped", defaultIDScheduler(taskToRun), taskTypeName, time.Now(), "Scheduler stopped", runnerCtx.Err())
			return
		}
	}
}

// submit attempts to submit a task to the pool and returns its ID and submission status.
// It retries submission up to retryScheduler times if the queue is full.
// Thread-safe via pool operations and logging.
func (s *Scheduler) submit(taskToRun interface{}, perExecutionCtx context.Context) (taskReferenceID string, submittedToPool bool) {
	taskReferenceID = defaultIDScheduler(taskToRun)
	taskTypeName := typeName(taskToRun)

	if s.pool == nil {
		s.logger.Info("Scheduler [%s]: Pool is nil, cannot submit task %s", s.name, taskTypeName)
		return taskReferenceID, false
	}

	var err error
	submissionSuccessful := true

	defer func() {
		if r := recover(); r != nil {
			s.logger.Info("CRITICAL: Panic during Scheduler [%s] task submission for %s: %v\n%s", s.name, taskTypeName, r, string(debug.Stack()))
			submissionSuccessful = false
		}
	}()

	switch task := taskToRun.(type) {
	case Task:
		err = s.pool.Submit(task)
		if err != nil {
			submissionSuccessful = false
			message := fmt.Sprintf("Error submitting Task %s to pool: %v", taskTypeName, err)
			if errors.Is(err, ErrQueueFull) {
				for i := 0; i < retryScheduler; i++ {
					time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
					err = s.pool.Submit(task)
					if err == nil {
						submissionSuccessful = true
						break
					}
				}
				if !submissionSuccessful {
					message = fmt.Sprintf("Pool queue full after %d retries, Task %s not submitted by scheduler [%s]", retryScheduler, taskTypeName, s.name)
					s.logger.Info("Scheduler [%s]: %s", s.name, message)
				}
			}
			s.logger.Info("Scheduler [%s]: %s", s.name, message)
		} else {
			s.logger.Info("Scheduler [%s]: Task %s submitted successfully", s.name, taskTypeName)
		}
	case TaskCtx:
		execCtx := perExecutionCtx
		if execCtx == nil {
			execCtx = context.Background()
		}
		select {
		case <-execCtx.Done():
			s.logger.Info("Scheduler [%s]: Execution context for Task %s is done (%v), not submitting", s.name, taskTypeName, execCtx.Err())
			return taskReferenceID, false
		default:
		}

		err = s.pool.SubmitCtx(execCtx, task)
		if err != nil {
			submissionSuccessful = false
			if !errors.Is(err, execCtx.Err()) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				s.logger.Info("Scheduler [%s]: Error submitting TaskCtx %s to pool: %v", s.name, taskTypeName, err)
			} else if err != nil {
				s.logger.Info("Scheduler [%s]: Submission of TaskCtx %s cancelled/timed out: %v", s.name, taskTypeName, err)
			}
		} else {
			s.logger.Info("Scheduler [%s]: TaskCtx %s submitted successfully", s.name, taskTypeName)
		}
	default:
		s.logger.Info("Scheduler [%s]: Unknown task type: %T", s.name, taskToRun)
		return taskReferenceID, false
	}
	return taskReferenceID, submissionSuccessful
}

// Do schedules one or more Tasks for execution in the worker pool.
// It returns an error if the scheduler is already running.
// Thread-safe via mutex and wait group.
// Example:
//
//	scheduler.Do(myTask) // Schedules a task
func (s *Scheduler) Do(ts ...Task) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return ErrSchedulerJobAlreadyRunning
	}
	for _, t := range ts {
		runnerLoopCtx, cancel := context.WithCancel(context.Background())
		s.runnerCancelFn = cancel
		s.task = t
		s.taskRunCtx = nil
		s.running = true
		s.runnerWg.Add(1)
		s.mu.Unlock()

		taskTypeName := typeName(t)
		s.emit("started", defaultIDScheduler(t), taskTypeName, time.Now(), "Scheduler job started", nil)
		go s.loop(runnerLoopCtx, t, nil)
	}
	return nil
}

// DoCtx schedules one or more TaskCtx tasks for execution with the given context.
// It returns an error if the scheduler is already running.
// Thread-safe via mutex and wait group.
// Example:
//
//	scheduler.DoCtx(ctx, myTaskCtx) // Schedules a context-aware task
func (s *Scheduler) DoCtx(taskExecCtx context.Context, ts ...TaskCtx) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return ErrSchedulerJobAlreadyRunning
	}

	for _, t := range ts {
		if taskExecCtx == nil {
			taskExecCtx = context.Background()
		}
		runnerLoopCtx, cancel := context.WithCancel(context.Background())
		s.runnerCancelFn = cancel
		s.task = t
		s.taskRunCtx = taskExecCtx
		s.running = true
		s.runnerWg.Add(1)
		s.mu.Unlock()

		taskTypeName := typeName(t)
		s.emit("started", defaultIDScheduler(t), taskTypeName, time.Now(), "Scheduler job started", nil)
		go s.loop(runnerLoopCtx, t, taskExecCtx)
	}
	return nil
}

// Terminate stops the scheduler and optionally shuts down the pool.
// It returns an error if the scheduler is not running.
// Thread-safe via mutex and wait group.
// Example:
//
//	scheduler.Terminate(true) // Stops scheduler and shuts down pool
func (s *Scheduler) Terminate(cancel bool) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrSchedulerNotRunning
	}
	taskTypeNameOnStop := typeName(s.task)
	taskIDOnStop := defaultIDScheduler(s.task)
	if s.runnerCancelFn != nil {
		s.runnerCancelFn()
	}
	s.running = false
	s.mu.Unlock()
	s.runnerWg.Wait()
	if cancel {
		s.pool.Shutdown(time.Second * 5)
	}
	s.emit("stopped", taskIDOnStop, taskTypeNameOnStop, time.Now(), "Scheduler job explicitly stopped", nil)
	return nil
}

// Stop stops the scheduler without shutting down the pool.
// It returns an error if the scheduler is not running.
// Thread-safe via mutex and wait group.
// Example:
//
//	scheduler.Stop() // Stops scheduler
func (s *Scheduler) Stop() error {
	return s.Terminate(false)
}

// Name returns the scheduler’s name.
// Thread-safe via immutable field access.
// Example:
//
//	name := scheduler.Name() // Retrieves scheduler name
func (s *Scheduler) Name() string {
	return s.name
}

// Running reports whether the scheduler is currently active.
// Thread-safe via mutex.
// Example:
//
//	if scheduler.Running() { ... } // Checks if scheduler is running
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

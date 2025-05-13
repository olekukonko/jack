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

var (
	ErrSchedulerJobAlreadyRunning = errors.New("scheduler: job is already running")
	ErrSchedulerNotRunning        = errors.New("scheduler: job is not running or already stopped")
	ErrSchedulerPoolNil           = errors.New("scheduler: pool cannot be nil")
	ErrSchedulerNameMissing       = errors.New("scheduler: name cannot be empty")
)

type Schedule struct {
	Type     string
	Name     string
	TaskID   string
	TaskType string
	Message  string
	Error    error
	Routine  Routine
	Time     time.Time
	NextRun  time.Time
}

type Cycle func(*Scheduling)

type Scheduling struct {
	observable Observable[Schedule]
}

func (s *Scheduling) Observable(obs Observable[Schedule]) {
	s.observable = obs
}

func SchedulingWithObservable(obs Observable[Schedule]) Cycle {
	return func(cfg *Scheduling) {
		cfg.observable = obs
	}
}

type Scheduler struct {
	name           string
	pool           *Pool
	routine        Routine
	cfg            Scheduling
	mu             sync.Mutex
	running        bool
	task           interface{}
	taskRunCtx     context.Context
	runnerCancelFn context.CancelFunc
	runnerWg       sync.WaitGroup
	logger         *ll.Logger
}

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

func (s *Scheduler) submit(taskToRun interface{}, perExecutionCtx context.Context) (taskReferenceID string, submittedToPool bool) {
	taskReferenceID = defaultIDScheduler(taskToRun)
	taskTypeName := typeName(taskToRun)

	if s.pool == nil {
		s.logger.Info("Scheduler [%s]: Pool is nil, cannot submit task %s. This should not happen.", s.name, taskTypeName)
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
				for i := 0; i < retryScheduler; i++ { // Retry up to 3 times
					time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
					err = s.pool.Submit(task)
					if err == nil {
						submissionSuccessful = true
						break
					}
				}
				if !submissionSuccessful {
					message = fmt.Sprintf("Pool queue full after retries, Task %s not submitted by scheduler [%s].", taskTypeName, s.name)
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
			s.logger.Info("Scheduler [%s]: Execution context for Task %s is done (%v), not submitting.", s.name, taskTypeName, execCtx.Err())
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
		s.pool.Shutdown(time.Second * 5) // Example timeout
	}
	s.emit("stopped", taskIDOnStop, taskTypeNameOnStop, time.Now(), "Scheduler job explicitly stopped.", nil)
	return nil
}

func (s *Scheduler) Stop() error {
	return s.Terminate(false)
}

func (s *Scheduler) Name() string {
	return s.name
}

func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

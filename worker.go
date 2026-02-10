package jack

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/ll"
)

// worker processes jobs from a task channel in a worker pool.
// It logs events and notifies an observable of job execution status.
// Thread-safe via channel operations and wait group synchronization.
type worker struct {
	id         int
	taskChan   <-chan job
	wg         *sync.WaitGroup
	observable Observable[Event]
	logger     *ll.Logger
}

// newWorker creates a new worker with the specified ID, task channel, wait group, and observable.
// It initializes a logger with a worker-specific namespace or a dummy logger if none is available.
// Example:
//
// w := newWorker(1, taskChan, wg, obs) // Creates worker with ID 1
func newWorker(id int, taskChan <-chan job, wg *sync.WaitGroup, obs Observable[Event]) *worker {
	var workerLogger *ll.Logger
	if logger != nil {
		workerLogger = logger.Namespace(fmt.Sprintf("worker-%d", id))
	} else {
		workerLogger = &ll.Logger{}
	}
	return &worker{
		id:         id,
		taskChan:   taskChan,
		wg:         wg,
		observable: obs,
		logger:     workerLogger,
	}
}

// start launches the worker in a new goroutine to process jobs from its task channel.
// It executes jobs, logs events, and notifies the observable of job start and completion.
// The worker exits when the task channel closes, decrementing the wait group as managed by the pool.
// Thread-safe via goroutine and channel operations.
// Example:
//
// w.start() // Starts worker to process jobs
func (w *worker) start() {
	go func() {
		defer func() {
			w.wg.Done()
			if w.logger != nil {
				w.logger.Info("Worker %d wait group done, goroutines: %d", w.id, runtime.NumGoroutine())
			}
		}()
		workerIDStr := fmt.Sprintf("worker-%d", w.id)
		if w.logger != nil {
			w.logger.Info("Worker %d starting, goroutines: %d", w.id, runtime.NumGoroutine())
		}
		for {
			select {
			case job, ok := <-w.taskChan:
				if !ok {
					if w.logger != nil {
						w.logger.Info("Worker %d task channel closed, exiting", w.id)
					}
					return
				}
				if job == nil {
					if w.logger != nil {
						w.logger.Info("Worker %d received nil job, skipping", w.id)
					}
					continue
				}
				taskID := job.ID()
				if taskID == "" {
					taskID = "unknown-task-" + ulid.Make().String()
					if w.logger != nil {
						w.logger.Info("Worker %d assigned ID %s to job with empty taskID", w.id, taskID)
					}
				}
				originalCtx := job.Context()
				if w.observable != nil {
					w.observable.Notify(Event{Type: "run", WorkerID: workerIDStr, TaskID: taskID, Time: time.Now()})
				}
				startTime := time.Now()
				errCh := make(chan error, 1)
				executeDone := make(chan struct{})
				go func() {
					defer func() {
						if r := recover(); r != nil {
							errCh <- &CaughtPanic{Val: r, Stack: debug.Stack()}
							if w.logger != nil {
								w.logger.Info("PANIC in task execution (Worker %d, TaskID %s): %v", w.id, taskID, r)
							}
						}
						close(executeDone)
					}()
					errCh <- job.Run(originalCtx)
				}()
				var err error
				select {
				case <-executeDone:
					err = <-errCh
				case <-originalCtx.Done():
					<-executeDone
					err = <-errCh
					if err == nil {
						err = originalCtx.Err()
					}
				}
				if w.observable != nil {
					w.observable.Notify(Event{
						Type:     "done",
						WorkerID: workerIDStr,
						TaskID:   taskID,
						Time:     time.Now(),
						Duration: time.Since(startTime),
						Err:      err,
					})
				}
			}
		}
	}()
}

// Logger sets the worker's logger to a new logger with a worker-specific namespace.
// It returns the worker for method chaining.
// Thread-safe as it updates the logger pointer.
// Example:
//
// w.Logger(extLogger) // Sets logger for worker
func (w *worker) Logger(extLogger *ll.Logger) *worker {
	if extLogger != nil {
		w.logger = extLogger.Namespace(fmt.Sprintf("worker-%d", w.id))
	}
	return w
}

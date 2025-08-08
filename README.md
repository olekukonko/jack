# Jack

Jack is a robust Go package for managing concurrent and asynchronous task execution. It provides a worker pool, task schedulers, runners, debouncers, mutex utilities, and observability tools to build scalable, reliable systems. Whether you're handling background jobs, rate-limiting function calls, or coordinating goroutines with context awareness and panic recovery, Jack simplifies concurrency while ensuring thread-safety, error handling, and monitoring.

### Why Use Jack?
- **Simplify Concurrency**: Manage worker pools, schedulers, and groups without boilerplate for channels, wait groups, or error propagation.
- **Robustness**: Built-in panic recovery, context cancellation, timeouts, and retry mechanisms prevent crashes and handle failures gracefully.
- **Observability**: Track task lifecycles (queued, running, done) with customizable event notifications for logging, metrics, or monitoring.
- **Flexibility**: Supports simple tasks, context-aware tasks, debouncing, and scheduling with intervals or limits.
- **Performance**: Efficient, lightweight design with configurable queues, workers, and backoffs for high-throughput applications.

Ideal for web servers, ETL pipelines, real-time systems, or any app needing reliable async processing.

## Features
- **Worker Pool**: Fixed-size goroutine pool for concurrent task execution with queueing and backpressure handling.
- **Runner**: Single-worker async queue for sequential task processing.
- **Scheduler**: Periodic or limited task submission to pools with retries on failures.
- **Group**: Coordinate multiple goroutines with error collection, limits, and context support.
- **Debouncer**: Group rapid function calls, executing only after inactivity or thresholds (e.g., for API rate-limiting).
- **Mutex Utilities**: Safe, context-aware locking with panic recovery and timeouts.
- **Observable**: Event-driven notifications for task events, extensible with custom observers.
- **Task Types**: Support for `Task` (simple), `TaskCtx` (context-aware), and `Identifiable` (custom IDs).
- **Error Handling**: Captures panics as `CaughtPanic` errors with optional stack traces.
- **Logging**: Integrated with a namespaced logger for detailed tracing.

## Installation
```bash
go get github.com/olekukonko/jack
```

## Quick Start
Here's a simple worker pool executing tasks concurrently:

```go
package main

import (
    "fmt"
    "time"
    "github.com/olekukonko/jack"
)

func main() {
    // Create a pool with 2 workers and a queue size of 10
    pool := jack.NewPool(2, jack.PoolingWithQueueSize(10))
    defer pool.Shutdown(5 * time.Second) // Graceful shutdown

    // Submit a simple task
    err := pool.Submit(jack.Func(func() error {
        fmt.Println("Task executed!")
        return nil
    }))
    if err != nil {
        fmt.Printf("Error: %v\n", err)
    }

    time.Sleep(time.Second) // Wait for execution
}
```

Run it, and you'll see the task output. Scale by adding more tasks or workers!

## Detailed Usage

### Worker Pool (`Pool`)
Manages concurrent task execution.

```go
// With observability
obs := jack.NewObservable[jack.Event]()
obs.Add(&logObserver{}) // Custom observer implementing Observer[Event]

pool := jack.NewPool(5, 
    jack.PoolingWithQueueSize(20),
    jack.PoolingWithObservable(obs),
    jack.PoolingWithIDGenerator(customIDFunc),
)

// Submit multiple tasks
pool.Submit(jack.Func(func() error { /* work */ return nil }))
pool.SubmitCtx(ctx, jack.FuncCtx(func(ctx context.Context) error { /* work with ctx */ return nil }))

// Monitor
fmt.Println("Queue size:", pool.QueueSize())
fmt.Println("Workers:", pool.Workers())
```

### Runner (`Runner`)
For async, sequential task processing.

```go
runner := jack.NewRunner(
    jack.WithRunnerQueueSize(15),
    jack.WithRunnerObservable(obs),
)

runner.Do(jack.Func(func() error { /* task */ return nil }))
runner.DoCtx(ctx, jack.FuncCtx(func(ctx context.Context) error { /* task */ return nil }))

defer runner.Shutdown(5 * time.Second)
```

### Scheduler (`Scheduler`)
Schedules tasks periodically or with limits.

```go
scheduler, err := jack.NewScheduler("my-scheduler", pool, jack.Routine{Interval: time.Second * 2, MaxRuns: 5},
    jack.SchedulingWithObservable(scheduleObs),
    jack.SchedulingWithRetry(5, time.Millisecond * 200),
)
if err != nil { /* handle */ }

scheduler.Do(jack.Func(func() error { fmt.Println("Scheduled!"); return nil }))

// Or context-aware
scheduler.DoCtx(ctx, jack.FuncCtx(func(ctx context.Context) error { /* ... */ return nil }))

// Stop later
scheduler.Stop()
```

### Group (`Group`)
Coordinates concurrent functions with error handling.

```go
group := jack.NewGroup().WithContext(ctx).WithLimit(3) // Limit to 3 concurrent

group.Go(func() error { /* goroutine 1 */ return nil })
group.GoCtx(func(ctx context.Context) error { /* goroutine 2 */ return nil })

go func() {
    for err := range group.Errors() {
        fmt.Printf("Error: %v\n", err)
    }
}()

group.Wait() // Block until done
```

Standalone `Go` for single functions:
```go
errCh := jack.Go(func() error { /* may panic or error */ return nil })
err := <-errCh // Receive error or nil
```

### Debouncer (`Debouncer`)
Groups rapid calls, executes after delay or thresholds.

```go
db := jack.NewDebouncer(
    jack.WithDebounceDelay(time.Millisecond * 500),
    jack.WithDebounceMaxCalls(10),
    jack.WithDebounceMaxWait(time.Second * 2),
)

db.Do(func() { fmt.Println("Debounced execution"); }) // Call multiple times, executes once after delay

db.Cancel() // Stop pending
db.Flush()  // Execute immediately
```

### Safely Utilities (`Safely`)
Safe locking with context and panic support.

```go
var mu jack.Safely

mu.Do(func() { /* critical section */ })

err := mu.Safe(func() error { /* may panic */ return nil })
if cp, ok := err.(*jack.CaughtPanic); ok {
    fmt.Printf("Panic: %v\nStack: %s\n", cp.Val, cp.Stack)
}

err = mu.SafeCtx(ctx, func() error { time.Sleep(time.Second); return nil }) // Supports timeouts
```

Standalone versions: `jack.Safe`, `jack.SafeCtx`, etc.

### Observable and Observers
For event notifications.

```go
obs := jack.NewObservable[string](3) // 3 workers for notifications
defer obs.Shutdown()

obs.Add(&myObserver{}) // Implements Observer[string]
obs.Notify("Event1", "Event2")
obs.Remove(&myObserver{})
```

Use with Pool/Scheduler/Runner for task events.

## API Reference
- **Types**: `Task`, `TaskCtx`, `Identifiable`, `Event`, `Schedule`, `Routine`, `CaughtPanic`.
- **Functions**: `NewPool`, `NewRunner`, `NewScheduler`, `NewDebouncer`, `NewObservable`, `NewGroup`, `Go`, `Safe`, `SafeCtx`, etc.
- **Options**: `PoolingWith...`, `WithRunner...`, `SchedulingWith...`, `WithDebounce...`.

See godoc for full details.

## Testing
Run comprehensive tests:
```bash
go test -v ./...
```

## Dependencies
- `github.com/oklog/ulid/v2`: Unique IDs.
- `github.com/olekukonko/ll`: Logging.

## Contributing
Contributions welcome! Open issues/PRs on GitHub.

## License
MIT License. See [LICENSE](LICENSE).
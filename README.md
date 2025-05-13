# Jack

Jack is a Go package for managing concurrent task execution with a worker pool, supporting logging and observability. It provides flexible components for task scheduling, execution, and event notification, suitable for building scalable, asynchronous systems.

## Features
- **Worker Pool**: Execute tasks concurrently with configurable worker counts and queue sizes.
- **Task Scheduling**: Schedule recurring or one-time tasks with customizable intervals and run limits.
- **Observability**: Emit events for task lifecycle stages (queued, running, done) with support for custom observers.
- **Error Handling**: Robust handling of task errors and panics, ensuring system stability.
- **Context Awareness**: Support for context-aware tasks with cancellation and timeouts.

## Installation
Install the package using Go modules:

```bash
go get github.com/olekukonko/jack
```

Replace `yourusername` with the actual repository owner.

## Components
- **Pool**: Manages a pool of workers to execute `Task` or `TaskCtx` tasks, with configurable queue size and observability.
- **Runner**: Executes tasks asynchronously using a single-worker queue, ideal for sequential processing.
- **Scheduler**: Schedules tasks for recurring or one-time execution, supporting intervals and maximum runs.
- **Group**: Coordinates concurrent function execution with error and panic handling, supporting worker limits and context cancellation.
- **Observable**: Provides an event notification system for observers to track task lifecycle events.

## Usage
Below are examples demonstrating key components of the `jack` package.

### Creating a Worker Pool
```go
package main

import (
	"fmt"
	"github.com/olekukonko/jack"
)

func main() {
	pool := jack.NewPool(2, jack.PoolingWithQueueSize(10))
	defer pool.Shutdown(5 * time.Second)

	task := jack.Func(func() error {
		fmt.Println("Task executed")
		return nil
	})
	if err := pool.Submit(task); err != nil {
		fmt.Printf("Submit failed: %v\n", err)
	}
}
```

### Scheduling a Task
```go
package main

import (
	"fmt"
	"github.com/olekukonko/jack"
	"time"
)

func main() {
	pool := jack.NewPool(1)
	defer pool.Shutdown(5 * time.Second)

	scheduler, _ := jack.NewScheduler("test", pool, jack.Routine{Interval: time.Second, MaxRuns: 3})
	task := jack.Func(func() error {
		fmt.Println("Scheduled task executed")
		return nil
	})
	if err := scheduler.Do(task); err != nil {
		fmt.Printf("Do failed: %v\n", err)
	}
	time.Sleep(4 * time.Second)
}
```

### Observing Events
```go
package main

import (
	"fmt"
	"github.com/olekukonko/jack"
)

type logObserver struct{}

func (l *logObserver) OnNotify(event jack.Event) {
	fmt.Printf("Event: %s, TaskID: %s\n", event.Type, event.TaskID)
}

func main() {
	obsable := jack.NewObservable[jack.Event](1)
	defer obsable.Shutdown()
	obsable.Add(&logObserver{})

	pool := jack.NewPool(1, jack.PoolingWithObservable(obsable))
	defer pool.Shutdown(5 * time.Second)

	task := jack.Func(func() error { return nil })
	pool.Submit(task)
}
```

## Testing
The package includes comprehensive tests for all components, verifying task execution, error handling, context cancellation, and concurrent operations. Run tests with:

```bash
go test -v ./...
```

## Dependencies
- `github.com/oklog/ulid/v2`: For generating unique task IDs.
- `github.com/olekukonko/ll`: For logging task execution and events.

## License
MIT License. See [LICENSE](LICENSE) for details.

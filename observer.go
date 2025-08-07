package jack

import (
	"github.com/olekukonko/ll"
	"sync"
)

// Observer defines the interface for objects that wish to receive notifications from an Observable.
// The type parameter T specifies the type of events or values that will be notified.
// Implementors must provide an OnNotify method to handle incoming events.
// Example:
//
//	type LogObserver struct {}
//	func (l *LogObserver) OnNotify(event string) {
//	    fmt.Printf("Received event: %s\n", event)
//	}
type Observer[T any] interface {
	OnNotify(value T) // Called when an event of type T is received
}

// Observable defines the interface for objects that can be observed by multiple Observer instances.
// It supports adding and removing observers, notifying them of one or more events, and shutting down cleanly.
// The type parameter T matches the event type for observers.
// Example:
//
//	obs := NewObservable[string]()
//	logObserver := &LogObserver{}
//	obs.Add(logObserver)
//	obs.Notify("Task completed")
//	obs.Shutdown()
type Observable[T any] interface {
	// Add registers one or more observers to receive notifications.
	Add(observers ...Observer[T])
	// Remove unregisters one or more observers.
	Remove(observers ...Observer[T])
	// Notify sends one or more events to all registered observers.
	Notify(events ...T)
	// Shutdown stops the observable and waits for all notifications to complete.
	Shutdown()
}

// notificationJob represents a single notification task for an observer.
// It pairs an observer with an event to be processed, used internally by the worker pool.
type notificationJob[T any] struct {
	observer Observer[T] // The observer to notify
	event    T           // The event to send to the observer
}

// eventObservable implements the Observable interface for type T.
// It uses a worker pool to process notifications asynchronously, ensuring non-blocking event delivery.
// Thread-safe via mutex, wait group, and channel operations.
// Example:
//
//	obs := NewObservable[string](3) // Create with 3 workers
//	obs.Add(&LogObserver{})
//	obs.Notify("Task started", "Task processing")
//	obs.Shutdown()
type eventObservable[T any] struct {
	observers  []Observer[T]           // List of registered observers
	mutex      sync.RWMutex            // Mutex for thread-safe access to observers list
	jobs       chan notificationJob[T] // Channel for queuing notification jobs
	shutdownCh chan struct{}           // Channel to signal shutdown
	workerWg   sync.WaitGroup          // WaitGroup to track active worker goroutines
	numWorkers int                     // Number of worker goroutines
	logger     *ll.Logger              // Logger for observability and error reporting
}

// NewObservable creates and returns a new Observable instance.
// It optionally accepts the number of worker goroutines for asynchronous notifications (defaults to 5 if not provided or invalid).
// Workers process notifications in parallel to avoid blocking the notifier, with panic recovery for observer errors.
// Parameters:
//   - numNotifyWorkers: Optional number of worker goroutines (positive integer). If not provided or invalid, defaults to 5.
//
// Returns:
//   - Observable[T]: A new Observable instance ready to accept observers and events.
//
// Example:
//
//	// Create an observable with 2 workers
//	obs := NewObservable[string](2)
//	logObserver := &LogObserver{}
//	obs.Add(logObserver)
//	obs.Notify("Event 1") // Sends "Event 1" to logObserver asynchronously
func NewObservable[T any](numNotifyWorkers ...int) Observable[T] {
	workers := defaultNumNotifyWorkers // Default worker count (typically 5)
	if len(numNotifyWorkers) > 0 && numNotifyWorkers[0] > 0 {
		workers = numNotifyWorkers[0] // Use provided worker count if valid
	}

	// Initialize observable with job channel and shutdown channel
	obs := &eventObservable[T]{
		jobs:       make(chan notificationJob[T], workers*10), // Buffer size scales with workers
		shutdownCh: make(chan struct{}),
		numWorkers: workers,
	}

	// Start worker goroutines to process notifications
	obs.workerWg.Add(workers)
	for i := 0; i < workers; i++ {
		go obs.process() // Launch worker goroutine
	}

	// Initialize logger with observable namespace
	obs.logger = logger.Namespace("observable")
	return obs
}

// process is a worker goroutine that handles notification jobs.
// It runs until shutdown, processing jobs from the channel and calling OnNotify on observers.
// Panics in observer methods are recovered and logged to prevent worker crashes.
func (o *eventObservable[T]) process() {
	defer o.workerWg.Done() // Signal worker completion on exit
	for {
		select {
		case job := <-o.jobs:
			func() {
				// Recover from panics in observer's OnNotify method
				defer func() {
					if r := recover(); r != nil {
						o.logger.Warn("PANIC in observer OnNotify (via worker): %v", r)
					}
				}()
				job.observer.OnNotify(job.event) // Deliver event to observer
			}()
		case <-o.shutdownCh:
			return // Exit worker on shutdown signal
		}
	}
}

// Add registers one or more observers to receive future notifications.
// It uses a write lock to safely append to the observer list, ensuring thread-safety for concurrent access.
// Parameters:
//   - observers: Variadic list of Observer[T] instances to register.
//
// Example:
//
//	type PrintObserver struct {}
//	func (p *PrintObserver) OnNotify(event string) { fmt.Println(event) }
//	obs := NewObservable[string]()
//	observer1, observer2 := &PrintObserver{}, &PrintObserver{}
//	obs.Add(observer1, observer2) // Both observers will receive future notifications
func (o *eventObservable[T]) Add(observers ...Observer[T]) {
	o.mutex.Lock() // Acquire write lock for thread-safety
	defer o.mutex.Unlock()

	for _, observer := range observers {
		o.observers = append(o.observers, observer) // Append each observer to the list
	}
}

// Remove unregisters one or more specified observers from receiving notifications.
// It uses a map for efficient lookup and rebuilds the observer list without the removed ones.
// This ensures all instances are removed even if duplicates exist, and is thread-safe.
// Parameters:
//   - observers: Variadic list of Observer[T] instances to unregister.
//
// Example:
//
//	obs := NewObservable[string]()
//	observer := &PrintObserver{}
//	obs.Add(observer)
//	obs.Remove(observer) // observer no longer receives notifications
func (o *eventObservable[T]) Remove(observers ...Observer[T]) {
	o.mutex.Lock() // Acquire write lock for thread-safety
	defer o.mutex.Unlock()

	// Create a map to mark observers for removal
	toRemove := make(map[Observer[T]]struct{}, len(observers))
	for _, obs := range observers {
		toRemove[obs] = struct{}{}
	}

	// Rebuild observer list excluding removed observers
	newObservers := make([]Observer[T], 0, len(o.observers))
	for _, obs := range o.observers {
		if _, found := toRemove[obs]; !found {
			newObservers = append(newObservers, obs)
		}
	}
	o.observers = newObservers
}

// Notify sends one or more events to all current observers asynchronously via workers.
// It takes a snapshot of observers to avoid issues if the list changes during notification.
// If shutdown is initiated mid-notification, remaining jobs are dropped with a log message.
// Parameters:
//   - events: Variadic list of events of type T to send to all observers.
//
// Example:
//
//	obs := NewObservable[string]()
//	obs.Add(&PrintObserver{})
//	obs.Notify("Task started", "Task completed") // All observers receive both events
func (o *eventObservable[T]) Notify(events ...T) {
	o.mutex.RLock() // Acquire read lock to safely copy observer list
	currentObservers := make([]Observer[T], len(o.observers))
	copy(currentObservers, o.observers)
	o.mutex.RUnlock()

	// Queue notification jobs for each event and observer
	for _, event := range events {
		for _, observer := range currentObservers {
			select {
			case o.jobs <- notificationJob[T]{observer: observer, event: event}: // Queue job for worker
			case <-o.shutdownCh:
				o.logger.Info("Observable shutting down, notification dropped.") // Log dropped notifications
				return
			}
		}
	}
}

// Shutdown gracefully stops the observable, closing channels and waiting for workers to finish.
// It prevents further notifications and ensures no jobs are processed after invocation.
// Calling Shutdown multiple times is idempotent and safe.
// Example:
//
//	obs := NewObservable[string]()
//	obs.Add(&PrintObserver{})
//	obs.Notify("Event 1")
//	obs.Shutdown() // Stops workers and prevents further notifications
func (o *eventObservable[T]) Shutdown() {
	o.mutex.Lock() // Acquire lock to check shutdown status
	select {
	case <-o.shutdownCh:
		o.mutex.Unlock()
		return // Already shut down, exit early
	default:
	}
	close(o.shutdownCh) // Signal workers to stop
	o.mutex.Unlock()

	o.workerWg.Wait() // Wait for all workers to complete
}

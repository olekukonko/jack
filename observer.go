// Package jack manages a worker pool for concurrent task execution with logging and observability.
package jack

import (
	"github.com/olekukonko/ll"
	"sync"
)

// Observer defines an interface for receiving notifications of type T.
// Implementations handle events sent by an Observable.
type Observer[T any] interface {
	// OnNotify processes a notification event.
	OnNotify(value T)
}

// Observable defines an interface for managing observers and sending notifications.
// It supports adding, removing, and notifying observers, as well as shutting down.
// Thread-safe via internal synchronization.
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
// It pairs an observer with an event to be processed.
type notificationJob[T any] struct {
	observer Observer[T] // Observer to notify
	event    T           // Event to send
}

// eventObservable implements the Observable interface for type T.
// It uses a worker pool to process notifications asynchronously.
// Thread-safe via mutex, wait group, and channel operations.
type eventObservable[T any] struct {
	observers  []Observer[T]           // Registered observers
	mutex      sync.RWMutex            // Protects observer list
	jobs       chan notificationJob[T] // Queue for notification tasks
	workerWg   sync.WaitGroup          // Waits for worker goroutines
	shutdownCh chan struct{}           // Signals shutdown
	numWorkers int                     // Number of worker goroutines
	logger     *ll.Logger              // Logger with observable namespace
}

// NewObservable creates a new Observable with the specified number of worker goroutines.
// If no number is provided or it’s non-positive, the default number is used.
// Thread-safe via initialization and worker goroutine setup.
// Example:
//
//	obs := NewObservable[int](50) // Creates observable with 50 workers
func NewObservable[T any](numNotifyWorkers ...int) Observable[T] {
	workers := defaultNumNotifyWorkers
	if len(numNotifyWorkers) > 0 && numNotifyWorkers[0] > 0 {
		workers = numNotifyWorkers[0]
	}

	obs := &eventObservable[T]{
		jobs:       make(chan notificationJob[T], workers*10),
		shutdownCh: make(chan struct{}),
		numWorkers: workers,
	}

	obs.workerWg.Add(workers)
	for i := 0; i < workers; i++ {
		go obs.process()
	}

	obs.logger = logger.Namespace("observable")
	return obs
}

// process runs a worker goroutine to handle notification jobs.
// It processes jobs from the queue until shutdown is signaled.
// Thread-safe via channel operations and wait group.
func (o *eventObservable[T]) process() {
	defer o.workerWg.Done()
	for {
		select {
		case job := <-o.jobs:
			func() {
				defer func() {
					if r := recover(); r != nil {
						o.logger.Warn("PANIC in observer OnNotify (via worker): %v", r)
					}
				}()
				job.observer.OnNotify(job.event)
			}()
		case <-o.shutdownCh:
			return
		}
	}
}

// Add registers one or more observers to receive notifications.
// Thread-safe via mutex.
// Example:
//
//	obs.Add(observer1, observer2) // Registers observers
func (o *eventObservable[T]) Add(observers ...Observer[T]) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	for _, observer := range observers {
		o.observers = append(o.observers, observer)
	}
}

// Remove unregisters one or more observers from receiving notifications.
// It removes the first occurrence of each observer.
// Thread-safe via mutex.
// Example:
//
//	obs.Remove(observer1) // Unregisters observer
func (o *eventObservable[T]) Remove(observers ...Observer[T]) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	for _, observer := range observers {
		for i, obs := range o.observers {
			if obs == observer {
				o.observers = append(o.observers[:i], o.observers[i+1:]...)
				return
			}
		}
	}
}

// Notify sends one or more events to all registered observers.
// Events are queued for asynchronous processing by workers.
// Thread-safe via mutex and channel operations.
// Example:
//
//	obs.Notify(event1, event2) // Sends events to observers
func (o *eventObservable[T]) Notify(events ...T) {
	o.mutex.RLock()
	currentObservers := make([]Observer[T], len(o.observers))
	copy(currentObservers, o.observers)
	o.mutex.RUnlock()

	for _, event := range events {
		for _, observer := range currentObservers {
			select {
			case o.jobs <- notificationJob[T]{observer: observer, event: event}:
			case <-o.shutdownCh:
				o.logger.Info("Observable shutting down, notification dropped")
				return
			}
		}
	}
}

// Shutdown stops the observable and waits for all workers to finish.
// It prevents new notifications and drops any during shutdown.
// Thread-safe via mutex and wait group.
// Example:
//
//	obs.Shutdown() // Shuts down observable
func (o *eventObservable[T]) Shutdown() {
	o.mutex.Lock()
	select {
	case <-o.shutdownCh:
		o.mutex.Unlock()
		return
	default:
	}
	close(o.shutdownCh)
	o.mutex.Unlock()

	o.workerWg.Wait()
}

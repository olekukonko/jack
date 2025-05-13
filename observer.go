package jack

import (
	"github.com/olekukonko/ll"
	"sync"
)

type Observer[T any] interface {
	OnNotify(value T)
}

type Observable[T any] interface {
	Add(observers ...Observer[T])
	Remove(observers ...Observer[T])
	Notify(events ...T)
	Shutdown()
}

type notificationJob[T any] struct {
	observer Observer[T]
	event    T
}

type eventObservable[T any] struct {
	observers  []Observer[T]
	mutex      sync.RWMutex
	jobs       chan notificationJob[T]
	workerWg   sync.WaitGroup
	shutdownCh chan struct{}
	numWorkers int
	logger     *ll.Logger
}

func NewObservable[T any](numNotifyWorkers ...int) Observable[T] {
	workers := defaultNumNotifyWorkers
	if len(numNotifyWorkers) > 0 && numNotifyWorkers[0] > 0 {
		workers = numNotifyWorkers[0]
	}

	obs := &eventObservable[T]{
		jobs:       make(chan notificationJob[T], workers*10), // Increased capacity to 50
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

func (o *eventObservable[T]) Add(observers ...Observer[T]) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, observer := range observers {
		o.observers = append(o.observers, observer)
	}
}

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

func (o *eventObservable[T]) Notify(events ...T) {
	o.mutex.RLock()
	currentObservers := make([]Observer[T], len(o.observers))
	copy(currentObservers, o.observers)
	o.mutex.RUnlock()

	// add events
	for _, event := range events {
		for _, observer := range currentObservers {
			select {
			case o.jobs <- notificationJob[T]{observer: observer, event: event}:
				// Job submitted
			case <-o.shutdownCh:
				o.logger.Info("Observable shutting down, notification dropped.")
				return
			}
		}
	}
}

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

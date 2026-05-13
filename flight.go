package jack

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
)

// FlightResult carries the outcome of a coalesced operation.
type FlightResult struct {
	Val    interface{}
	Shared bool // true if caller waited for a leader to complete
}

// Flight deduplicates concurrent executions by key. The first caller
// becomes the leader and runs fn directly; waiters block until the
// leader finishes and receive the same result.
type Flight struct {
	mu      sync.Mutex
	calls   map[string]*fcall
	metrics *FlightMetrics
	opts    flightOpt
	logger  *ll.Logger
}

// flightOpt holds configuration options for Flight.
type flightOpt struct {
	observable Observable[Event]
}

// FlightOption is a functional option for configuring Flight.
type FlightOption func(*flightOpt)

// FlightWithObservable sets an observable for flight events.
func FlightWithObservable(obs Observable[Event]) FlightOption {
	return func(opts *flightOpt) { opts.observable = obs }
}

// FlightMetrics holds atomic counters for observability.
type FlightMetrics struct {
	Leaders  atomic.Uint64
	Waiters  atomic.Uint64
	Served   atomic.Uint64
	Canceled atomic.Uint64
	InFlight atomic.Int64
}

// NewFlight creates a coalescing flight group.
func NewFlight(opts ...FlightOption) *Flight {
	options := flightOpt{}
	for _, opt := range opts {
		opt(&options)
	}
	f := &Flight{
		calls:   make(map[string]*fcall),
		metrics: &FlightMetrics{},
		opts:    options,
	}
	if logger != nil {
		f.logger = logger.Namespace("flight")
	} else {
		f.logger = &ll.Logger{}
	}
	return f
}

// Metrics returns a snapshot of current metrics.
func (f *Flight) Metrics() *FlightMetrics {
	return f.metrics
}

// Do executes fn once per key. The leader receives Shared=false;
// concurrent callers receive Shared=true after the leader finishes.
// If fn panics, the panic is recovered and returned as a *CaughtPanic to
// every waiter and the leader itself.
func (f *Flight) Do(key string, fn func() (interface{}, error)) (res FlightResult, err error) {
	return f.DoCtx(context.Background(), key, fn)
}

// DoCtx executes fn once per key. The leader receives Shared=false;
// concurrent callers receive Shared=true after the leader finishes.
// If fn panics, the panic is recovered and returned as a *CaughtPanic to
// every waiter and the leader itself.
func (f *Flight) DoCtx(ctx context.Context, key string, fn func() (interface{}, error)) (res FlightResult, err error) {
	f.mu.Lock()
	if c, ok := f.calls[key]; ok {
		f.metrics.Waiters.Add(1)
		f.mu.Unlock()

		select {
		case <-c.done:
			res = FlightResult{Val: c.val, Shared: true}
			err = c.err
			f.metrics.Served.Add(1)
			if f.opts.observable != nil {
				f.opts.observable.Notify(Event{Type: "flight_served", TaskID: key, Time: time.Now()})
			}
			return
		case <-ctx.Done():
			f.metrics.Canceled.Add(1)
			if f.opts.observable != nil {
				f.opts.observable.Notify(Event{Type: "flight_canceled", TaskID: key, Time: time.Now(), Err: ctx.Err()})
			}
			err = ctx.Err()
			return
		}
	}

	c := &fcall{done: make(chan struct{})}
	f.calls[key] = c
	f.metrics.Leaders.Add(1)
	f.metrics.InFlight.Add(1)
	f.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			err = &CaughtPanic{
				Value: r,
				Stack: debug.Stack(),
			}
			f.logger.Warn("Flight panic for key %s: %v", key, r)
		}
		c.val = res.Val
		c.err = err
		close(c.done)

		f.mu.Lock()
		delete(f.calls, key)
		f.mu.Unlock()

		f.metrics.InFlight.Add(-1)

		if f.opts.observable != nil {
			typ := "flight_done"
			if err != nil {
				typ = "flight_failed"
			}
			f.opts.observable.Notify(Event{Type: typ, TaskID: key, Time: time.Now(), Err: err})
		}
	}()

	res.Val, err = fn()
	return
}

// Forget drops an in-flight key so the next caller starts fresh.
// Existing waiters continue blocking on the current leader.
func (f *Flight) Forget(key string) {
	f.mu.Lock()
	delete(f.calls, key)
	f.mu.Unlock()
}

// InFlight reports whether the given key has an active leader.
func (f *Flight) InFlight(key string) bool {
	f.mu.Lock()
	_, ok := f.calls[key]
	f.mu.Unlock()
	return ok
}

// fcall tracks a single in-flight operation.
type fcall struct {
	done chan struct{}
	val  interface{}
	err  error
}

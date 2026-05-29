package jack

import (
	"context"
	"sync"
	"sync/atomic"
)

// OnceGroup coalesces in-flight duplicate requests for the same key.
// The first call for a key executes fn; subsequent calls wait and
// receive the same result. Panics are propagated to all waiters.
type OnceGroup[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*onceGroupCall[V]

	inFlight  atomic.Int64
	coalesced atomic.Int64
	completed atomic.Int64
	panics    atomic.Int64
}

type onceGroupCall[V any] struct {
	val      V
	err      error
	panicked bool
	panicVal any
	done     chan struct{}
}

// Do executes fn for key if no in-flight call exists. Otherwise,
// it waits for the existing call to complete and shares its result.
// The returned bool indicates whether the result was shared.
// Context cancellation affects only the waiter; the underlying
// execution continues.
func (g *OnceGroup[K, V]) Do(ctx context.Context, key K, fn func() (V, error)) (V, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*onceGroupCall[V])
	}
	c, shared := g.m[key]
	if !shared {
		c = &onceGroupCall[V]{done: make(chan struct{})}
		g.m[key] = c
		g.inFlight.Add(1)
		g.mu.Unlock()

		func() {
			defer func() {
				if r := recover(); r != nil {
					c.panicked = true
					c.panicVal = r
					g.panics.Add(1)
				}
				close(c.done)
				g.mu.Lock()
				delete(g.m, key)
				g.inFlight.Add(-1)
				g.completed.Add(1)
				g.mu.Unlock()
			}()
			c.val, c.err = fn()
		}()
	} else {
		g.coalesced.Add(1)
		g.mu.Unlock()
	}

	select {
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err(), shared
	case <-c.done:
	}

	if c.panicked {
		panic(c.panicVal)
	}

	return c.val, c.err, shared
}

// OnceGroupMetrics holds observable counters.
type OnceGroupMetrics struct {
	InFlight  int64
	Coalesced int64
	Completed int64
	Panics    int64
}

// Metrics returns coalescing statistics.
func (g *OnceGroup[K, V]) Metrics() OnceGroupMetrics {
	return OnceGroupMetrics{
		InFlight:  g.inFlight.Load(),
		Coalesced: g.coalesced.Load(),
		Completed: g.completed.Load(),
		Panics:    g.panics.Load(),
	}
}

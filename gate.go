package jack

import (
	"context"
	"sync"
)

// Gate is a reusable barrier that can be opened, closed, or pulsed.
// When open, all waiters pass; when closed, waiters block.
// Pulse wakes current waiters but remains closed.
type Gate struct {
	mu      sync.Mutex
	opened  bool
	waiters []chan struct{}
}

// Open allows all current and future waiters to proceed.
func (g *Gate) Open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.opened = true
	for _, ch := range g.waiters {
		close(ch)
	}
	g.waiters = g.waiters[:0]
}

// Close prevents future waiters from proceeding.
func (g *Gate) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.opened = false
}

// Pulse wakes all current waiters; the gate remains closed.
func (g *Gate) Pulse() {
	g.mu.Lock()
	defer g.mu.Unlock()
	waiters := g.waiters
	g.waiters = g.waiters[:0]
	for _, ch := range waiters {
		close(ch)
	}
}

// Wait blocks until the gate is opened.
func (g *Gate) Wait() {
	g.mu.Lock()
	if g.opened {
		g.mu.Unlock()
		return
	}
	ch := make(chan struct{})
	g.waiters = append(g.waiters, ch)
	g.mu.Unlock()
	<-ch
}

// TryWait returns true if the gate is currently open.
func (g *Gate) TryWait() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.opened
}

// WaitCtx blocks until the gate is opened or the context is cancelled.
func (g *Gate) WaitCtx(ctx context.Context) error {
	g.mu.Lock()
	if g.opened {
		g.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	g.waiters = append(g.waiters, ch)
	g.mu.Unlock()

	select {
	case <-ctx.Done():
		g.removeWaiter(ch)
		return ctx.Err()
	case <-ch:
		return nil
	}
}

func (g *Gate) removeWaiter(ch chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, w := range g.waiters {
		if w == ch {
			g.waiters = append(g.waiters[:i], g.waiters[i+1:]...)
			return
		}
	}
}

// IsOpen reports whether the gate is currently open.
func (g *Gate) IsOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.opened
}

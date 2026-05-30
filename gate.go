package jack

import (
	"context"
	"sync"
	"sync/atomic"
)

// gateSignal wraps a channel with once-only close.
type gateSignal struct {
	ch        chan struct{}
	closeOnce sync.Once
}

func (s *gateSignal) Close() {
	if s != nil {
		s.closeOnce.Do(func() { close(s.ch) })
	}
}

// Gate is a reusable barrier. Open lets all waiters pass; Close blocks
// future waiters. Pulse wakes current waiters but remains closed.
type Gate struct {
	opened   atomic.Bool
	signal   atomic.Pointer[gateSignal]
	pulseGen atomic.Uint64
	once     sync.Once
}

func (g *Gate) init() {
	g.once.Do(func() {
		if g.signal.Load() == nil {
			g.signal.Store(&gateSignal{ch: make(chan struct{})})
		}
	})
}

// Open allows all current and future waiters to proceed.
func (g *Gate) Open() {
	g.opened.Store(true)
	old := g.signal.Load()
	if old != nil {
		old.Close()
	}
	newSig := &gateSignal{ch: make(chan struct{})}
	newSig.Close()
	g.signal.Store(newSig)
}

// Close prevents future waiters from proceeding.
func (g *Gate) Close() {
	g.opened.Store(false)
	g.signal.Store(&gateSignal{ch: make(chan struct{})})
}

// Pulse wakes all current waiters; the gate remains closed.
func (g *Gate) Pulse() {
	g.pulseGen.Add(1)
	newSig := &gateSignal{ch: make(chan struct{})}
	old := g.signal.Swap(newSig)
	g.opened.Store(false)
	old.Close()
}

// Wait blocks until the gate is opened or pulsed.
func (g *Gate) Wait() {
	g.init()
	startGen := g.pulseGen.Load()
	for {
		if g.opened.Load() {
			return
		}
		sig := g.signal.Load()
		if sig == nil {
			if g.opened.Load() {
				return
			}
			continue
		}
		<-sig.ch
		if g.pulseGen.Load() != startGen {
			return
		}
	}
}

// TryWait returns true if the gate is currently open.
func (g *Gate) TryWait() bool {
	return g.opened.Load()
}

// WaitCtx blocks until the gate is opened, pulsed, or ctx is cancelled.
func (g *Gate) WaitCtx(ctx context.Context) error {
	g.init()
	startGen := g.pulseGen.Load()
	for {
		if g.opened.Load() {
			return nil
		}
		sig := g.signal.Load()
		if sig == nil {
			if g.opened.Load() {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				continue
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sig.ch:
			if g.pulseGen.Load() != startGen {
				return nil
			}
		}
	}
}

// IsOpen reports whether the gate is currently open.
func (g *Gate) IsOpen() bool {
	return g.opened.Load()
}

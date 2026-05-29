package jack

import (
	"sync"
	"sync/atomic"
)

// Latch is a one-shot signal. It starts closed; once Open() is called,
// it remains open forever. Safe for concurrent use.
type Latch struct {
	initOnce sync.Once
	openOnce sync.Once
	done     chan struct{}
	opened   atomic.Bool
}

func (l *Latch) init() {
	l.initOnce.Do(func() {
		l.done = make(chan struct{})
	})
}

// Open signals the latch. Subsequent calls are no-ops.
func (l *Latch) Open() {
	l.init()
	l.openOnce.Do(func() {
		l.opened.Store(true)
		close(l.done)
	})
}

// Wait blocks until the latch is opened.
func (l *Latch) Wait() {
	l.init()
	<-l.done
}

// TryWait returns true if the latch is already open.
func (l *Latch) TryWait() bool {
	l.init()
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// IsOpen reports whether Open has been called.
func (l *Latch) IsOpen() bool {
	return l.opened.Load()
}

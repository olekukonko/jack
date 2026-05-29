package jack

import (
	"context"
	"sync"
	"sync/atomic"
)

// WaitGroup is a reusable goroutine coordinator. Unlike sync.WaitGroup,
// it can be reused immediately after Wait returns.
type WaitGroup struct {
	mu      sync.Mutex
	count   int
	waiters []chan struct{}
	closed  atomic.Bool
}

// Add adds n to the wait group count.
func (wg *WaitGroup) Add(n int) {
	wg.mu.Lock()
	wg.count += n
	wg.closed.Store(false)
	wg.mu.Unlock()
}

// Done decrements the wait group count.
func (wg *WaitGroup) Done() {
	wg.mu.Lock()
	wg.count--
	if wg.count < 0 {
		wg.mu.Unlock()
		panic("jack.WaitGroup: negative count")
	}
	if wg.count == 0 {
		wg.closed.Store(true)
		for _, ch := range wg.waiters {
			close(ch)
		}
		wg.waiters = wg.waiters[:0]
	}
	wg.mu.Unlock()
}

// Go starts fn in a new goroutine and tracks its completion.
func (wg *WaitGroup) Go(fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn()
	}()
}

// Wait blocks until all tracked goroutines complete.
func (wg *WaitGroup) Wait() {
	wg.mu.Lock()
	if wg.count == 0 {
		wg.mu.Unlock()
		return
	}
	ch := make(chan struct{})
	wg.waiters = append(wg.waiters, ch)
	wg.mu.Unlock()
	<-ch
}

// TryWait returns true if no goroutines are currently tracked.
func (wg *WaitGroup) TryWait() bool {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	return wg.count == 0
}

// Waiters returns the number of active goroutines.
func (wg *WaitGroup) Waiters() int {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	return wg.count
}

// WaitCtx blocks until all tracked goroutines complete or the context is cancelled.
func (wg *WaitGroup) WaitCtx(ctx context.Context) error {
	wg.mu.Lock()
	if wg.count == 0 {
		wg.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	wg.waiters = append(wg.waiters, ch)
	wg.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// IsDone reports whether all tracked goroutines have completed.
func (wg *WaitGroup) IsDone() bool {
	return wg.closed.Load()
}

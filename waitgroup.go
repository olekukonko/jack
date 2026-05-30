// waitgroup.go
package jack

import (
	"context"
	"sync"
)

// WaitGroup is a reusable goroutine coordinator. Unlike sync.WaitGroup,
// it can be reused immediately after Wait returns.
type WaitGroup struct {
	mu      sync.Mutex
	count   int
	signal  chan struct{}
	started bool
}

// Add adds n to the wait group count.
func (wg *WaitGroup) Add(n int) {
	wg.mu.Lock()
	wg.count += n
	if wg.count < 0 {
		wg.mu.Unlock()
		panic("jack.WaitGroup: negative count")
	}
	if n > 0 {
		wg.started = true
	}
	if wg.count > 0 && wg.signal == nil {
		wg.signal = make(chan struct{})
	}
	if wg.count == 0 && wg.signal != nil {
		close(wg.signal)
		wg.signal = nil
	}
	wg.mu.Unlock()
}

// Done decrements the wait group count.
func (wg *WaitGroup) Done() {
	wg.Add(-1)
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
	ch := wg.signal
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

// WaitCtx blocks until all tracked goroutines complete or ctx is cancelled.
func (wg *WaitGroup) WaitCtx(ctx context.Context) error {
	wg.mu.Lock()
	if wg.count == 0 {
		wg.mu.Unlock()
		return nil
	}
	ch := wg.signal
	wg.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// IsDone reports whether all tracked goroutines have completed.
// Returns false for a zero-value WaitGroup that has never been used.
func (wg *WaitGroup) IsDone() bool {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	return wg.started && wg.count == 0
}

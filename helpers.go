package jack

import (
	"context"
	"sync"
	"time"
)

// Wait runs fn in a goroutine and blocks until it completes or ctx is cancelled.
func Wait(ctx context.Context, fn func()) error {
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// WaitTimeout runs fn in a goroutine and blocks until it completes or timeout elapses.
func WaitTimeout(timeout time.Duration, fn func()) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return Wait(ctx, fn)
}

// Execute runs fn in a goroutine and returns its error or ctx cancellation.
func Execute(ctx context.Context, fn func() error) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// Repeat runs fn at interval until ctx is cancelled or fn returns an error.
func Repeat(ctx context.Context, interval time.Duration, fn func(context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := fn(ctx); err != nil {
				return err
			}
		}
	}
}

// Parallel runs fn for i in [0,n) concurrently. Returns the first error or nil.
func Parallel(ctx context.Context, n int, fn func(context.Context, int) error) error {
	if n <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := fn(ctx, i); err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

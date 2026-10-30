package jack

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupRunsAndClosesErrors(t *testing.T) {
	g := NewGroup()
	var ran atomic.Int32
	for i := 0; i < 3; i++ {
		g.Go(func() error {
			ran.Add(1)
			return nil
		})
	}
	g.Wait()

	if got := ran.Load(); got != 3 {
		t.Fatalf("expected 3 functions to run, got %d", got)
	}
	if _, ok := <-g.Errors(); ok {
		t.Fatal("expected Errors channel to be closed after Wait")
	}
}

func TestGroupPropagatesFirstError(t *testing.T) {
	wantErr := errors.New("boom")
	g := NewGroup().WithContext(context.Background())
	g.Go(func() error { return wantErr })
	g.Wait()

	select {
	case err := <-g.Errors():
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected %v, got %v", wantErr, err)
		}
	default:
		t.Fatal("expected an error on the Errors channel")
	}
}

func TestGroupCancelsContextOnError(t *testing.T) {
	g := NewGroup().WithContext(context.Background())
	g.Go(func() error { return errors.New("fail fast") })

	g.GoCtx(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return nil
		}
	})
	g.Wait()

	if g.ctx.Err() == nil {
		t.Fatal("expected group context to be cancelled after the first error")
	}
}

func TestGroupRecoversPanics(t *testing.T) {
	g := NewGroup()
	g.Go(func() error { panic("group panic") })
	g.Wait()

	select {
	case err := <-g.Errors():
		cp, ok := err.(*CaughtPanic)
		if !ok {
			t.Fatalf("expected *CaughtPanic, got %T: %v", err, err)
		}
		if cp.Value != "group panic" {
			t.Fatalf("unexpected panic value %v", cp.Value)
		}
	default:
		t.Fatal("expected panic to be reported on the Errors channel")
	}
}

func TestGroupGoCtxRequiresContext(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when GoCtx is used without WithContext")
		}
	}()
	NewGroup().GoCtx(func(context.Context) error { return nil })
}

// TestGroupLimitDoesNotDeadlockGoCtx guards against the double semaphore
// acquisition that made GoCtx self-deadlock with WithLimit(n).
func TestGroupLimitDoesNotDeadlockGoCtx(t *testing.T) {
	g := NewGroup().WithContext(context.Background()).WithLimit(1)
	for i := 0; i < 4; i++ {
		g.GoCtx(func(context.Context) error { return nil })
	}

	done := make(chan struct{})
	go func() {
		g.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("GoCtx deadlocked when WithLimit limits concurrency")
	}
}

func TestGroupLimitCapsConcurrency(t *testing.T) {
	const limit = 2
	g := NewGroup().WithLimit(limit)

	var inFlight, maxSeen atomic.Int32
	for i := 0; i < 8; i++ {
		g.Go(func() error {
			cur := inFlight.Add(1)
			for {
				prev := maxSeen.Load()
				if cur <= prev || maxSeen.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			return nil
		})
	}
	g.Wait()

	if got := maxSeen.Load(); got > limit {
		t.Fatalf("concurrency limit violated: %d > %d", got, limit)
	}
	if got := inFlight.Load(); got != 0 {
		t.Fatalf("expected all workers to release their slot, inFlight=%d", got)
	}
}

// TestGroupReuseAfterWaitDoesNotPanic covers the send-on-closed-channel panic
// that happened when a Group produced an error after Wait had closed Errors.
func TestGroupReuseAfterWaitDoesNotPanic(t *testing.T) {
	g := NewGroup()
	g.Go(func() error { return nil })
	g.Wait()

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reusing a Group after Wait panicked: %v", r)
			}
		}()
		g.Go(func() error { return errors.New("late error") })
		g.Wait()
	}()
}

func TestGroupGoCtxSkipsCancelledGroup(t *testing.T) {
	g := NewGroup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.WithContext(ctx)

	var called atomic.Bool
	g.GoCtx(func(context.Context) error {
		called.Store(true)
		return nil
	})
	g.Wait()

	if called.Load() {
		t.Fatal("expected cancelled context to prevent the function from running")
	}
	select {
	case err := <-g.Errors():
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	default:
		t.Fatal("expected the cancellation to be reported")
	}
}

func TestGoStandalone(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	errCh := Go(func() error { defer wg.Done(); return nil })
	go func() {
		defer wg.Done()
		for range errCh {
		}
	}()
	wg.Wait()

	panicCh := Go(func() error { panic("standalone") })
	var seen error
	for err := range panicCh {
		seen = err
	}
	if cp, ok := seen.(*CaughtPanic); !ok || cp.Value != "standalone" {
		t.Fatalf("expected CaughtPanic from standalone Go, got %T: %v", seen, seen)
	}
}

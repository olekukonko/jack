package jack

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitGroup_GoWait(t *testing.T) {
	var wg WaitGroup
	var n atomic.Int32
	wg.Go(func() {
		n.Add(1)
	})
	wg.Go(func() {
		n.Add(1)
	})
	wg.Wait()
	if n.Load() != 2 {
		t.Fatalf("expected 2, got %d", n.Load())
	}
}

func TestWaitGroup_AddDone(t *testing.T) {
	var wg WaitGroup
	wg.Add(2)
	go func() {
		time.Sleep(50 * time.Millisecond)
		wg.Done()
	}()
	go func() {
		time.Sleep(100 * time.Millisecond)
		wg.Done()
	}()
	wg.Wait()
}

func TestWaitGroup_Reuse(t *testing.T) {
	var wg WaitGroup
	wg.Go(func() {})
	wg.Wait()

	wg.Go(func() {})
	wg.Wait()
}

func TestWaitGroup_TryWait(t *testing.T) {
	var wg WaitGroup
	if !wg.TryWait() {
		t.Fatal("should be empty")
	}
	wg.Go(func() {
		time.Sleep(100 * time.Millisecond)
	})
	if wg.TryWait() {
		t.Fatal("should not be empty")
	}
	wg.Wait()
	if !wg.TryWait() {
		t.Fatal("should be empty after wait")
	}
}

func TestWaitGroup_Waiters(t *testing.T) {
	var wg WaitGroup
	if wg.Waiters() != 0 {
		t.Fatal("should be 0")
	}
	wg.Go(func() {
		time.Sleep(100 * time.Millisecond)
	})
	if wg.Waiters() != 1 {
		t.Fatalf("expected 1, got %d", wg.Waiters())
	}
	wg.Wait()
	if wg.Waiters() != 0 {
		t.Fatalf("expected 0, got %d", wg.Waiters())
	}
}

func TestWaitGroup_WaitCtx(t *testing.T) {
	var wg WaitGroup
	wg.Go(func() {
		time.Sleep(200 * time.Millisecond)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := wg.WaitCtx(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestWaitGroup_WaitCtx_Success(t *testing.T) {
	var wg WaitGroup
	wg.Go(func() {
		time.Sleep(50 * time.Millisecond)
	})

	ctx := context.Background()
	if err := wg.WaitCtx(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitGroup_IsDone(t *testing.T) {
	var wg WaitGroup
	if wg.IsDone() {
		t.Fatal("should not be done initially")
	}
	wg.Go(func() {})
	wg.Wait()
	if !wg.IsDone() {
		t.Fatal("should be done")
	}
	wg.Go(func() {})
	if wg.IsDone() {
		t.Fatal("should not be done after new go")
	}
	wg.Wait()
	if !wg.IsDone() {
		t.Fatal("should be done again")
	}
}

func TestWaitGroup_NegativeCount(t *testing.T) {
	var wg WaitGroup
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	wg.Done()
}

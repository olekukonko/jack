package jack

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGate_OpenClose(t *testing.T) {
	var g Gate
	g.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.Wait()
	}()

	time.Sleep(50 * time.Millisecond)
	g.Open()
	wg.Wait()

	if !g.IsOpen() {
		t.Fatal("gate should be open")
	}
}

func TestGate_Pulse(t *testing.T) {
	var g Gate
	g.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		g.Wait()
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	g.Pulse()
	<-done
	wg.Wait()

	if g.IsOpen() {
		t.Fatal("gate should be closed after pulse")
	}
}

func TestGate_MultiplePulses(t *testing.T) {
	var g Gate
	g.Close()

	var count atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Wait()
			count.Add(1)
		}()
	}

	time.Sleep(50 * time.Millisecond)
	g.Pulse()
	wg.Wait()

	if count.Load() != 3 {
		t.Fatalf("expected 3, got %d", count.Load())
	}
	if g.IsOpen() {
		t.Fatal("should be closed")
	}
}

func TestGate_TryWait(t *testing.T) {
	var g Gate
	if g.TryWait() {
		t.Fatal("should not be open initially")
	}
	g.Open()
	if !g.TryWait() {
		t.Fatal("should be open")
	}
	g.Close()
	if g.TryWait() {
		t.Fatal("should be closed")
	}
}

func TestGate_WaitCtx(t *testing.T) {
	var g Gate
	g.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := g.WaitCtx(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestGate_WaitCtx_Open(t *testing.T) {
	var g Gate
	g.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		g.Open()
	}()

	ctx := context.Background()
	if err := g.WaitCtx(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGate_ConcurrentWaiters(t *testing.T) {
	var g Gate
	g.Close()

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			g.Wait()
		}()
	}

	time.Sleep(50 * time.Millisecond)
	g.Open()
	wg.Wait()
}

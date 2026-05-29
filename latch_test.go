package jack

import (
	"sync"
	"testing"
	"time"
)

func TestLatch_OpenWait(t *testing.T) {
	var l Latch
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		l.Wait()
	}()

	time.Sleep(50 * time.Millisecond)
	if l.IsOpen() {
		t.Fatal("latch should not be open yet")
	}

	l.Open()
	wg.Wait()

	if !l.IsOpen() {
		t.Fatal("latch should be open")
	}
}

func TestLatch_TryWait(t *testing.T) {
	var l Latch
	if l.TryWait() {
		t.Fatal("should not be open")
	}
	l.Open()
	if !l.TryWait() {
		t.Fatal("should be open")
	}
}

func TestLatch_OpenIdempotent(t *testing.T) {
	var l Latch
	l.Open()
	l.Open()
	if !l.IsOpen() {
		t.Fatal("should be open")
	}
}

func TestLatch_WaitBeforeOpen(t *testing.T) {
	var l Latch
	done := make(chan struct{})
	go func() {
		l.Wait()
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("should not be done yet")
	default:
	}

	l.Open()
	<-done
}

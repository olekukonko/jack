package jack

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestCoalescer_Basic(t *testing.T) {
	var batches atomic.Int32
	flush := func(items []interface{}) error {
		batches.Add(1)
		if len(items) != 3 {
			t.Errorf("batch size = %d, want 3", len(items))
		}
		return nil
	}

	c := NewCoalescer(flush, 3, time.Second)
	for i := 0; i < 3; i++ {
		if err := c.Add(i); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	if batches.Load() != 1 {
		t.Errorf("flushes = %d, want 1", batches.Load())
	}
}

func TestCoalescer_TimerFlush(t *testing.T) {
	var batches atomic.Int32
	flush := func(items []interface{}) error {
		batches.Add(1)
		return nil
	}

	c := NewCoalescer(flush, 100, 50*time.Millisecond)
	c.Add(1)
	c.Add(2)

	if batches.Load() != 0 {
		t.Error("expected no flush before timer")
	}

	time.Sleep(100 * time.Millisecond)
	if batches.Load() != 1 {
		t.Errorf("flushes = %d, want 1", batches.Load())
	}
}

func TestCoalescer_Close(t *testing.T) {
	var flushed []interface{}
	flush := func(items []interface{}) error {
		flushed = items
		return nil
	}

	c := NewCoalescer(flush, 100, time.Second)
	c.Add(1)
	c.Add(2)

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(flushed) != 2 {
		t.Errorf("flushed = %d, want 2", len(flushed))
	}
	if err := c.Add(3); err == nil {
		t.Error("expected error after close")
	}
}

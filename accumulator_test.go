package jack

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAccumulator_ExplicitFlushOnly(t *testing.T) {
	var batches atomic.Int32

	flush := func(items []any) error {
		batches.Add(1)
		return nil
	}

	acc := NewAccumulator(flush)
	defer acc.Close()

	for i := 0; i < 5; i++ {
		if err := acc.Add(i); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	// Without size or time options, no auto-flush
	if batches.Load() != 0 {
		t.Fatalf("expected 0 auto-flushes, got %d", batches.Load())
	}

	if err := acc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if batches.Load() != 1 {
		t.Fatalf("expected 1 flush, got %d", batches.Load())
	}
}

func TestAccumulator_AutoFlushOnSize(t *testing.T) {
	var batches atomic.Int32
	var lastSize int

	flush := func(items []any) error {
		batches.Add(1)
		lastSize = len(items)
		return nil
	}

	acc := NewAccumulator(flush, AccumulatorWithMaxSize(3))
	defer acc.Close()

	acc.Add(1) //nolint:errcheck
	acc.Add(2) //nolint:errcheck
	if batches.Load() != 0 {
		t.Fatalf("expected 0 flushes, got %d", batches.Load())
	}

	if err := acc.Add(3); err != nil {
		t.Fatalf("add: %v", err)
	}
	if batches.Load() != 1 {
		t.Fatalf("expected 1 size flush, got %d", batches.Load())
	}
	if lastSize != 3 {
		t.Fatalf("expected batch size 3, got %d", lastSize)
	}
}

func TestAccumulator_AutoFlushOnTime(t *testing.T) {
	var batches atomic.Int32

	flush := func(items []any) error {
		batches.Add(1)
		return nil
	}

	acc := NewAccumulator(flush, AccumulatorWithMaxWait(50*time.Millisecond))
	defer acc.Close()

	acc.Add(1) //nolint:errcheck
	acc.Add(2) //nolint:errcheck
	if batches.Load() != 0 {
		t.Fatalf("expected 0 flushes before timer, got %d", batches.Load())
	}

	waitForCondition(t, time.Second, func() bool { return batches.Load() == 1 },
		"expected 1 timer flush")
}

func TestAccumulator_SizeOrTime_WhicheverFirst_SizeWins(t *testing.T) {
	var batches atomic.Int32

	flush := func(items []any) error {
		batches.Add(1)
		return nil
	}

	// maxSize=3, maxWait=1s — size should fire first
	acc := NewAccumulator(flush, AccumulatorWithMaxSize(3), AccumulatorWithMaxWait(time.Second))
	defer acc.Close()

	acc.Add(1) //nolint:errcheck
	acc.Add(2) //nolint:errcheck
	if err := acc.Add(3); err != nil {
		t.Fatalf("add: %v", err)
	}

	if batches.Load() != 1 {
		t.Fatalf("expected size-triggered flush, got %d batches", batches.Load())
	}

	m := acc.Metrics()
	if m.SizeFlushes.Load() != 1 {
		t.Fatalf("expected SizeFlushes=1, got %d", m.SizeFlushes.Load())
	}
	if m.TimerFlushes.Load() != 0 {
		t.Fatalf("expected TimerFlushes=0, got %d", m.TimerFlushes.Load())
	}
}

func TestAccumulator_SizeOrTime_WhicheverFirst_TimeWins(t *testing.T) {
	var batches atomic.Int32

	flush := func(items []any) error {
		batches.Add(1)
		return nil
	}

	// maxSize=100, maxWait=50ms — time should fire first
	acc := NewAccumulator(flush, AccumulatorWithMaxSize(100), AccumulatorWithMaxWait(50*time.Millisecond))
	defer acc.Close()

	acc.Add(1) //nolint:errcheck
	acc.Add(2) //nolint:errcheck

	waitForCondition(t, time.Second, func() bool { return batches.Load() == 1 },
		"expected timer-triggered flush")

	m := acc.Metrics()
	if m.TimerFlushes.Load() != 1 {
		t.Fatalf("expected TimerFlushes=1, got %d", m.TimerFlushes.Load())
	}
}

func TestAccumulator_TimerResetsOnFlush(t *testing.T) {
	var batches atomic.Int32

	flush := func(items []any) error {
		batches.Add(1)
		return nil
	}

	acc := NewAccumulator(flush, AccumulatorWithMaxWait(100*time.Millisecond))
	defer acc.Close()

	// First batch
	acc.Add(1) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return batches.Load() == 1 },
		"expected first timer flush")

	// Second batch — timer should restart from first add
	acc.Add(2) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return batches.Load() == 2 },
		"expected second timer flush")
}

func TestAccumulator_EmptyFlush(t *testing.T) {
	var batches atomic.Int32
	flush := func(items []any) error { batches.Add(1); return nil }

	acc := NewAccumulator(flush)
	defer acc.Close()

	if err := acc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if batches.Load() != 0 {
		t.Fatalf("expected 0 flushes on empty, got %d", batches.Load())
	}
}

func TestAccumulator_CloseFlushes(t *testing.T) {
	var flushed []any
	flush := func(items []any) error { flushed = items; return nil }

	acc := NewAccumulator(flush)
	acc.Add(1) //nolint:errcheck
	acc.Add(2) //nolint:errcheck

	if err := acc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(flushed) != 2 {
		t.Fatalf("expected 2 items, got %d", len(flushed))
	}
	if err := acc.Add(3); err == nil {
		t.Fatal("expected error after close")
	}
}

func TestAccumulator_FlushError(t *testing.T) {
	wantErr := errors.New("flush failed")
	flush := func(items []any) error { return wantErr }

	acc := NewAccumulator(flush)
	defer acc.Close()

	acc.Add(1) //nolint:errcheck
	err := acc.Flush()
	if err != wantErr {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestAccumulator_ConcurrentAddFlush(t *testing.T) {
	var totalFlushed atomic.Int32
	flush := func(items []any) error {
		totalFlushed.Add(int32(len(items)))
		return nil
	}

	acc := NewAccumulator(flush, AccumulatorWithMaxSize(50))
	defer acc.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				acc.Add(id*100 + j) //nolint:errcheck
			}
		}(i)
	}
	wg.Wait()

	// Some auto-flushes may have happened; flush remainder
	acc.Flush() //nolint:errcheck

	if totalFlushed.Load() != 100 {
		t.Fatalf("expected 100 items flushed, got %d", totalFlushed.Load())
	}
}

func TestAccumulator_PanicRecovery(t *testing.T) {
	flush := func(items []any) error { panic("flush panic") }
	acc := NewAccumulator(flush)
	defer acc.Close()

	acc.Add(1) //nolint:errcheck
	err := acc.Flush()
	if err == nil {
		t.Fatal("expected error from panic")
	}
	cp, ok := err.(*CaughtPanic)
	if !ok || cp.Value != "flush panic" {
		t.Fatalf("expected CaughtPanic, got %T: %v", err, err)
	}
}

func TestAccumulator_Len(t *testing.T) {
	acc := NewAccumulator(func([]any) error { return nil })
	defer acc.Close()

	if n := acc.Len(); n != 0 {
		t.Fatalf("expected empty accumulator, got %d", n)
	}
	for i := 0; i < 4; i++ {
		if err := acc.Add(i); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if n := acc.Len(); n != 4 {
		t.Fatalf("expected 4 buffered items, got %d", n)
	}
	if err := acc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if n := acc.Len(); n != 0 {
		t.Fatalf("expected 0 buffered items after flush, got %d", n)
	}
}

func TestAccumulator_CloseTwice(t *testing.T) {
	acc := NewAccumulator(func([]any) error { return nil })

	if err := acc.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := acc.Close(); err != ErrAccumulatorClosed {
		t.Fatalf("expected ErrAccumulatorClosed on second close, got %v", err)
	}
}

func TestAccumulator_CloseStopsTimer(t *testing.T) {
	var batches atomic.Int32
	flush := func(items []any) error { batches.Add(1); return nil }

	acc := NewAccumulator(flush, AccumulatorWithMaxWait(30*time.Millisecond))
	if err := acc.Add(1); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := acc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if batches.Load() != 1 {
		t.Fatalf("expected close to flush pending batch, got %d flushes", batches.Load())
	}

	// The pending timer must not fire after Close.
	time.Sleep(60 * time.Millisecond)
	if batches.Load() != 1 {
		t.Fatalf("timer fired after close: %d flushes", batches.Load())
	}
	if n := acc.Metrics().TimerFlushes.Load(); n != 0 {
		t.Fatalf("expected TimerFlushes=0 after close, got %d", n)
	}
}

func TestAccumulator_NilFlushPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when flush function is nil")
		}
	}()
	NewAccumulator(nil) //nolint:errcheck
}

func TestAccumulator_NonPositiveOptionsIgnored(t *testing.T) {
	var batches atomic.Int32
	flush := func(items []any) error { batches.Add(1); return nil }

	acc := NewAccumulator(flush,
		AccumulatorWithMaxSize(0),
		AccumulatorWithMaxWait(0),
	)
	defer acc.Close()

	for i := 0; i < 10; i++ {
		if err := acc.Add(i); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if batches.Load() != 0 {
		t.Fatalf("expected no auto-flush with non-positive options, got %d", batches.Load())
	}
	if err := acc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if batches.Load() != 1 {
		t.Fatalf("expected 1 explicit flush, got %d", batches.Load())
	}
}

func TestAccumulator_Observable(t *testing.T) {
	collector := newEventCollector()
	obs := NewObservable[Event](1)
	defer obs.Shutdown()
	obs.Add(collector)

	acc := NewAccumulator(func([]any) error { return nil }, AccumulatorWithObservable(obs))
	defer acc.Close()

	if err := acc.Add(1); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := acc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	waitForCondition(t, time.Second,
		func() bool { return len(collector.findEvents("", "accumulator_flush")) == 1 },
		"expected an accumulator_flush event")

	failing := NewAccumulator(func([]any) error { return errors.New("boom") },
		AccumulatorWithObservable(obs))
	defer failing.Close() //nolint:errcheck

	if err := failing.Add(1); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := failing.Flush(); err == nil {
		t.Fatal("expected flush error")
	}
	waitForCondition(t, time.Second,
		func() bool { return len(collector.findEvents("", "accumulator_flush_failed")) == 1 },
		"expected an accumulator_flush_failed event")
}

func TestAccumulator_AddDuringSlowFlush(t *testing.T) {
	var delivered atomic.Int64
	flush := func(items []any) error {
		time.Sleep(2 * time.Millisecond)
		delivered.Add(int64(len(items)))
		return nil
	}

	acc := NewAccumulator(flush, AccumulatorWithMaxSize(4), AccumulatorWithMaxWait(3*time.Millisecond))

	const total = 100
	for i := 0; i < total; i++ {
		if err := acc.Add(i); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := acc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if delivered.Load() != total {
		t.Fatalf("expected %d items delivered, got %d", total, delivered.Load())
	}
}

func TestAccumulator_NoItemLossUnderConcurrency(t *testing.T) {
	const (
		writers = 16
		perG    = 200
		total   = writers * perG
	)

	var mu sync.Mutex
	seen := make(map[int]int, total)
	var emptyBatches atomic.Int64

	flush := func(items []any) error {
		if len(items) == 0 {
			emptyBatches.Add(1)
		}
		mu.Lock()
		for _, item := range items {
			seen[item.(int)]++
		}
		mu.Unlock()
		return nil
	}

	acc := NewAccumulator(flush, AccumulatorWithMaxSize(7), AccumulatorWithMaxWait(2*time.Millisecond))

	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if err := acc.Add(g*perG + i); err != nil {
					t.Errorf("add: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if err := acc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if n := emptyBatches.Load(); n != 0 {
		t.Fatalf("flush invoked %d times with an empty batch", n)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Fatalf("expected %d distinct items, got %d", total, len(seen))
	}
	for item, count := range seen {
		if count != 1 {
			t.Fatalf("item %d flushed %d times", item, count)
		}
	}

	m := acc.Metrics()
	if m.ItemsAdded.Load() != total {
		t.Fatalf("expected ItemsAdded=%d, got %d", total, m.ItemsAdded.Load())
	}
	if m.ItemsFlushed.Load() != total {
		t.Fatalf("expected ItemsFlushed=%d, got %d", total, m.ItemsFlushed.Load())
	}
	// Every flush is either size-triggered, timer-triggered, or the explicit
	// Close flush (which contributes only if items remained buffered).
	if extra := m.Flushes.Load() - m.SizeFlushes.Load() - m.TimerFlushes.Load(); extra > 1 {
		t.Fatalf("Flushes=%d, SizeFlushes=%d, TimerFlushes=%d (unexplained flushes=%d)",
			m.Flushes.Load(), m.SizeFlushes.Load(), m.TimerFlushes.Load(), extra)
	}
}

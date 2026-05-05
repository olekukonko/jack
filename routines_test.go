package jack

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRoutinesGo(t *testing.T) {
	r := NewRoutines()
	defer r.Stop()

	done := make(chan struct{})
	id := r.Spawn("work", func(ctx context.Context) error {
		close(done)
		return nil
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("goroutine did not run")
	}
	r.Wait()

	info, ok := r.Info(id)
	if !ok {
		t.Fatal("expected info for goroutine")
	}
	if info.State != RoutineDone {
		t.Fatalf("expected Done, got %v", info.State)
	}
	if info.Label != "work" {
		t.Fatalf("expected label 'work', got %q", info.Label)
	}
}

func TestRoutinesStop(t *testing.T) {
	r := NewRoutines()

	started := make(chan struct{})
	r.Spawn("long", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	<-started
	r.Stop()

	if r.Active() != 0 {
		t.Fatalf("expected 0 active after Stop, got %d", r.Active())
	}
}

func TestRoutinesPanicRecovery(t *testing.T) {
	var panicked atomic.Bool
	r := NewRoutines(
		RoutineWithOnPanic(func(info RoutineInfo) {
			panicked.Store(true)
		}),
	)
	defer r.Stop()

	r.Spawn("boom", func(ctx context.Context) error {
		panic("test panic")
	})
	r.Wait()

	if !panicked.Load() {
		t.Fatal("expected panic callback to fire")
	}

	for _, info := range r.List() {
		if info.Label == "boom" {
			if info.State != RoutinePanicked {
				t.Fatalf("expected Panicked, got %v", info.State)
			}
			if info.Stack == nil {
				t.Fatal("expected non-nil stack trace")
			}
			return
		}
	}
	t.Fatal("goroutine 'boom' not found in list")
}

func TestRoutinesError(t *testing.T) {
	r := NewRoutines()
	defer r.Stop()

	testErr := errors.New("task failed")
	id := r.Spawn("fail", func(ctx context.Context) error {
		return testErr
	})
	r.Wait()

	info, _ := r.Info(id)
	if info.Err != testErr {
		t.Fatalf("expected testErr, got %v", info.Err)
	}
	if r.Metrics().Failed.Load() != 1 {
		t.Fatal("expected 1 failed")
	}
}

func TestRoutinesCancellation(t *testing.T) {
	r := NewRoutines()

	id := r.Spawn("cancel", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	r.Stop()

	info, ok := r.Info(id)
	if !ok {
		t.Fatal("expected info")
	}
	if info.State != RoutineCancelled {
		t.Fatalf("expected Cancelled, got %v", info.State)
	}
	if r.Metrics().Cancelled.Load() != 1 {
		t.Fatal("expected 1 cancelled")
	}
}

func TestRoutinesGoWithContext(t *testing.T) {
	r := NewRoutines()
	defer r.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	id := r.SpawnCtx(ctx, "ctx-aware", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	cancel() // cancel the caller's context, not the tracker's
	r.Wait()

	info, _ := r.Info(id)
	if info.State == RoutineRunning {
		t.Fatal("goroutine should have exited after caller context cancelled")
	}
}

func TestRoutinesGoBackground(t *testing.T) {
	r := NewRoutines()
	defer r.Stop()

	var runs atomic.Int32
	id := r.Background("bg", 3, func(ctx context.Context) error {
		if runs.Add(1) < 3 {
			return errors.New("retry me")
		}
		return nil
	})

	r.Wait()
	info, _ := r.Info(id)
	_ = info
	if runs.Load() < 3 {
		t.Fatalf("expected at least 3 runs for background goroutine, got %d", runs.Load())
	}
}

func TestRoutinesList(t *testing.T) {
	r := NewRoutines()
	defer r.Stop()

	for i := 0; i < 5; i++ {
		r.Spawn("task", func(ctx context.Context) error { return nil })
	}
	r.Wait()

	list := r.List()
	if len(list) != 5 {
		t.Fatalf("expected 5 entries in list, got %d", len(list))
	}
}

func TestRoutinesMetrics(t *testing.T) {
	r := NewRoutines()
	defer r.Stop()

	for i := 0; i < 4; i++ {
		r.Spawn("t", func(ctx context.Context) error { return nil })
	}
	r.Wait()

	m := r.Metrics()
	if m.Spawned.Load() != 4 {
		t.Fatalf("expected 4 spawned, got %d", m.Spawned.Load())
	}
	if m.Completed.Load() != 4 {
		t.Fatalf("expected 4 completed, got %d", m.Completed.Load())
	}
	if m.Active.Load() != 0 {
		t.Fatalf("expected 0 active, got %d", m.Active.Load())
	}
}

func TestRoutinesOnDone(t *testing.T) {
	var seen atomic.Int32
	r := NewRoutines(
		RoutineWithOnDone(func(_ RoutineInfo) { seen.Add(1) }),
	)
	defer r.Stop()

	for i := 0; i < 3; i++ {
		r.Spawn("x", func(ctx context.Context) error { return nil })
	}
	r.Wait()

	if seen.Load() != 3 {
		t.Fatalf("expected 3 onDone callbacks, got %d", seen.Load())
	}
}

func TestRoutinesDefaultSingleton(t *testing.T) {
	if defaultRoutines == nil {
		t.Fatal("DefaultRoutines should be non-nil")
	}
	done := make(chan struct{})
	defaultRoutines.Spawn("singleton-test", func(ctx context.Context) error {
		close(done)
		return nil
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("DefaultRoutines goroutine did not run")
	}
}

func TestRoutinesConcurrentStress(t *testing.T) {
	r := NewRoutines()
	defer r.Stop()

	var wg atomic.Int32
	total := 100
	wg.Store(int32(total))
	done := make(chan struct{})

	for i := 0; i < total; i++ {
		r.Spawn("stress", func(ctx context.Context) error {
			time.Sleep(time.Millisecond)
			if wg.Add(-1) == 0 {
				close(done)
			}
			return nil
		})
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stress test timed out")
	}
	r.Wait()

	if r.Metrics().Spawned.Load() != uint64(total) {
		t.Fatalf("expected %d spawned, got %d", total, r.Metrics().Spawned.Load())
	}
}

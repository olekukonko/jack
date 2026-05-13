package jack

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlight_Do_Serial(t *testing.T) {
	f := NewFlight()
	fn := func() (interface{}, error) { return "ok", nil }

	res, err := f.DoCtx(context.Background(), "k", fn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Shared {
		t.Error("first call must be leader")
	}
	if res.Val != "ok" {
		t.Errorf("val = %v, want ok", res.Val)
	}

	// After completion the key is gone; next call is a new leader.
	res, err = f.DoCtx(context.Background(), "k", fn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Shared {
		t.Error("second call after completion must be leader")
	}
}

func TestFlight_Do_Coalesce(t *testing.T) {
	f := NewFlight()
	var calls atomic.Int32

	fn := func() (interface{}, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "result", nil
	}

	var wg sync.WaitGroup
	results := make(chan FlightResult, 10)
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := f.DoCtx(context.Background(), "k", fn)
			results <- res
			errors <- err
		}()
	}

	wg.Wait()
	close(results)
	close(errors)

	if calls.Load() != 1 {
		t.Errorf("leader calls = %d, want 1", calls.Load())
	}

	var leaders, waiters int
	for res := range results {
		if res.Shared {
			waiters++
		} else {
			leaders++
		}
		if res.Val != "result" {
			t.Errorf("val = %v, want result", res.Val)
		}
	}
	for err := range errors {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if leaders != 1 {
		t.Errorf("leaders = %d, want 1", leaders)
	}
	if waiters != 9 {
		t.Errorf("waiters = %d, want 9", waiters)
	}
}

func TestFlight_Do_ContextCancel(t *testing.T) {
	f := NewFlight()
	leaderStarted := make(chan struct{})
	leaderBlock := make(chan struct{})

	fn := func() (interface{}, error) {
		close(leaderStarted)
		<-leaderBlock
		return "done", nil
	}

	go f.DoCtx(context.Background(), "cancel-k", fn)
	<-leaderStarted

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.DoCtx(ctx, "cancel-k", fn)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}

	close(leaderBlock)
}

func TestFlight_Do_ErrorPropagation(t *testing.T) {
	f := NewFlight()
	want := errors.New("boom")

	fn := func() (interface{}, error) { return nil, want }

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := f.DoCtx(context.Background(), "err-k", fn)
			if !res.Shared {
				// leader
				if err != want {
					t.Errorf("leader err = %v, want %v", err, want)
				}
				return
			}
			if err != want {
				t.Errorf("waiter err = %v, want %v", err, want)
			}
		}()
	}
	wg.Wait()
}

func TestFlight_Do_PanicRecovery(t *testing.T) {
	f := NewFlight()
	leaderBlock := make(chan struct{})
	fn := func() (interface{}, error) {
		<-leaderBlock
		panic("boom")
	}

	var shared atomic.Int32
	var wg sync.WaitGroup

	// Start leader first so it establishes the in-flight key.
	go func() {
		res, err := f.DoCtx(context.Background(), "panic-k", fn)
		if res.Shared {
			t.Error("leader should not be shared")
		}
		var cp *CaughtPanic
		if !errors.As(err, &cp) || cp.Value != "boom" {
			t.Errorf("leader error = %v, want panic: boom", err)
		}
	}()

	// Let leader enter the map.
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := f.DoCtx(context.Background(), "panic-k", fn)
			if !res.Shared {
				t.Error("waiter should be shared")
			}
			var cp *CaughtPanic
			if !errors.As(err, &cp) || cp.Value != "boom" {
				t.Errorf("waiter error = %v, want panic: boom", err)
			}
			shared.Add(1)
		}()
	}

	// Let waiters join.
	time.Sleep(20 * time.Millisecond)
	close(leaderBlock)

	wg.Wait()

	if shared.Load() != 2 {
		t.Errorf("shared = %d, want 2", shared.Load())
	}
}

func TestFlight_Forget(t *testing.T) {
	f := NewFlight()
	block := make(chan struct{})
	var secondLeader atomic.Bool

	fn := func() (interface{}, error) {
		<-block
		return "v1", nil
	}

	go f.DoCtx(context.Background(), "forget-k", fn)
	time.Sleep(20 * time.Millisecond)

	f.Forget("forget-k")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		res, _ := f.DoCtx(context.Background(), "forget-k", func() (interface{}, error) {
			secondLeader.Store(true)
			return "v2", nil
		})
		if res.Val != "v2" {
			t.Errorf("val = %v, want v2", res.Val)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	if !secondLeader.Load() {
		t.Error("expected second leader after Forget")
	}
	close(block)
	wg.Wait()
}

func TestFlight_Metrics(t *testing.T) {
	f := NewFlight()
	leaderBlock := make(chan struct{})

	fn := func() (interface{}, error) {
		<-leaderBlock
		return "ok", nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	go f.DoCtx(ctx, "m-k", fn)
	time.Sleep(20 * time.Millisecond)

	go f.DoCtx(context.Background(), "m-k", fn)
	go f.DoCtx(ctx, "m-k", fn)

	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	close(leaderBlock)
	time.Sleep(50 * time.Millisecond)

	m := f.Metrics()
	if m.Leaders.Load() != 1 {
		t.Errorf("leaders = %d, want 1", m.Leaders.Load())
	}
	if m.Waiters.Load() < 1 {
		t.Errorf("waiters = %d, want >=1", m.Waiters.Load())
	}
	if m.InFlight.Load() != 0 {
		t.Errorf("in-flight = %d, want 0", m.InFlight.Load())
	}
}

func BenchmarkFlight_Do_Uncontended(b *testing.B) {
	f := NewFlight()
	fn := func() (interface{}, error) { return 1, nil }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.DoCtx(context.Background(), "k", fn)
	}
}

func BenchmarkFlight_Do_Contended(b *testing.B) {
	f := NewFlight()
	fn := func() (interface{}, error) {
		time.Sleep(time.Millisecond)
		return 1, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f.DoCtx(context.Background(), "k", fn)
		}
	})
}

func BenchmarkFlight_Do_ManyKeys(b *testing.B) {
	f := NewFlight()
	fn := func() (interface{}, error) { return 1, nil }

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = string([]byte{byte(i >> 8), byte(i)})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			f.DoCtx(context.Background(), keys[i%1000], fn)
			i++
		}
	})
}

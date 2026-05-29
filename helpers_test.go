package jack

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWait(t *testing.T) {
	ctx := context.Background()
	var done bool
	err := Wait(ctx, func() {
		time.Sleep(50 * time.Millisecond)
		done = true
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !done {
		t.Fatal("fn should have run")
	}
}

func TestWait_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Wait(ctx, func() {
		time.Sleep(200 * time.Millisecond)
	})
	if err != context.Canceled {
		t.Fatalf("expected canceled, got %v", err)
	}
}

func TestWaitTimeout(t *testing.T) {
	err := WaitTimeout(50*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
	})
	if err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestExecute(t *testing.T) {
	ctx := context.Background()
	err := Execute(ctx, func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecute_Error(t *testing.T) {
	ctx := context.Background()
	expected := errors.New("boom")
	err := Execute(ctx, func() error {
		return expected
	})
	if err != expected {
		t.Fatalf("expected %v, got %v", expected, err)
	}
}

func TestExecute_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Execute(ctx, func() error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	if err != context.Canceled {
		t.Fatalf("expected canceled, got %v", err)
	}
}

func TestRepeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	var count int
	err := Repeat(ctx, 50*time.Millisecond, func(ctx context.Context) error {
		count++
		return nil
	})
	if err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if count < 2 {
		t.Fatalf("expected at least 2 iterations, got %d", count)
	}
}

func TestRepeat_Error(t *testing.T) {
	ctx := context.Background()
	expected := errors.New("boom")
	err := Repeat(ctx, 10*time.Millisecond, func(ctx context.Context) error {
		return expected
	})
	if err != expected {
		t.Fatalf("expected %v, got %v", expected, err)
	}
}

func TestParallel(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64
	err := Parallel(ctx, 10, func(ctx context.Context, i int) error {
		sum.Add(int64(i))
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum.Load() != 45 {
		t.Fatalf("expected 45, got %d", sum.Load())
	}
}

func TestParallel_FailFast(t *testing.T) {
	ctx := context.Background()
	expected := errors.New("boom")
	err := Parallel(ctx, 10, func(ctx context.Context, i int) error {
		if i == 5 {
			return expected
		}
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	if err != expected {
		t.Fatalf("expected %v, got %v", expected, err)
	}
}

func TestParallel_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Parallel(ctx, 5, func(ctx context.Context, i int) error {
		return nil
	})
	if err != context.Canceled {
		t.Fatalf("expected canceled, got %v", err)
	}
}

func TestParallel_Zero(t *testing.T) {
	ctx := context.Background()
	err := Parallel(ctx, 0, func(ctx context.Context, i int) error {
		return errors.New("should not run")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

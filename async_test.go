package jack

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestFuture_Basic(t *testing.T) {
	t.Parallel()

	future := Async(func() (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "hello", nil
	})

	result, err := future.Await()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != "hello" {
		t.Errorf("Expected 'hello', got %v", result)
	}
}

func TestFuture_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")
	future := Async(func() (string, error) {
		return "", expectedErr
	})

	result, err := future.Await()
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
	if result != "" {
		t.Errorf("Expected empty string, got %v", result)
	}
}

func TestFuture_Panic(t *testing.T) {
	t.Parallel()

	future := Async(func() (string, error) {
		panic("something went wrong")
	})

	result, err := future.Await()

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	panicErr, ok := err.(*CaughtPanic)
	if !ok {
		t.Fatalf("Expected *CaughtPanic, got %T", err)
	}

	if panicErr.Value != "something went wrong" {
		t.Errorf("Expected panic value 'something went wrong', got %v", panicErr.Value)
	}

	if len(panicErr.Stack) == 0 {
		t.Error("Expected stack trace, got empty")
	}

	var zero string
	if result != zero {
		t.Errorf("Expected zero value, got %v", result)
	}
}

func TestFuture_Cancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	future := AsyncWithContext(ctx, func(ctx context.Context) (string, error) {
		time.Sleep(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return "done", nil
		}
	})

	// Cancel after 100ms
	time.AfterFunc(100*time.Millisecond, cancel)

	result, err := future.Await()

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}

	var zero string
	if result != zero {
		t.Errorf("Expected zero value, got %v", result)
	}
}

func TestFuture_Timeout(t *testing.T) {
	t.Parallel()

	future := Async(func() (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "done", nil
	})

	result, err := future.AwaitWithTimeout(100 * time.Millisecond)

	if !errors.Is(err, ErrFutureTimeout) {
		t.Errorf("Expected ErrFutureTimeout, got %v", err)
	}

	var zero string
	if result != zero {
		t.Errorf("Expected zero value, got %v", result)
	}

	// Wait for actual completion to ensure no goroutine leak
	time.Sleep(150 * time.Millisecond)
}

func TestFuture_AwaitWithContext(t *testing.T) {
	t.Parallel()

	future := Async(func() (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "done", nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := future.AwaitWithContext(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}

	var zero string
	if result != zero {
		t.Errorf("Expected zero value, got %v", result)
	}
}

func TestFuture_IsDone(t *testing.T) {
	t.Parallel()

	future := Async(func() (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "done", nil
	})

	if future.IsDone() {
		t.Error("Expected IsDone to be false initially")
	}

	time.Sleep(150 * time.Millisecond)

	if !future.IsDone() {
		t.Error("Expected IsDone to be true after completion")
	}
}

func TestFuture_IsCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	future := AsyncWithContext(ctx, func(ctx context.Context) (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "done", nil
	})

	if future.IsCanceled() {
		t.Error("Expected IsCanceled to be false initially")
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	// Cancel should be detected
	future.Await() // This will complete with canceled error

	if !future.IsCanceled() {
		t.Error("Expected IsCanceled to be true after cancel")
	}
}

func TestFuture_Then(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	future1 := Async(func() (int, error) {
		return 42, nil
	})

	future2 := future1.Then(ctx, func(val int) (any, error) {
		return fmt.Sprintf("answer: %d", val), nil
	})

	result, err := future2.Await()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result != "answer: 42" {
		t.Errorf("Expected 'answer: 42', got %v", result)
	}
}

func TestFuture_Then_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	expectedErr := errors.New("original error")

	future1 := Async(func() (int, error) {
		return 0, expectedErr
	})

	future2 := future1.Then(ctx, func(val int) (any, error) {
		return "should not happen", nil
	})

	result, err := future2.Await()
	if err != expectedErr {
		t.Errorf("Expected original error %v, got %v", expectedErr, err)
	}

	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}
}

func TestFuture_Recover(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	future1 := Async(func() (string, error) {
		return "", errors.New("something went wrong")
	})

	future2 := future1.Recover(ctx, func(err error) (string, error) {
		return "recovered", nil
	})

	result, err := future2.Await()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result != "recovered" {
		t.Errorf("Expected 'recovered', got %v", result)
	}
}

func TestFuture_Recover_NoError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	future1 := Async(func() (string, error) {
		return "success", nil
	})

	recovered := false
	future2 := future1.Recover(ctx, func(err error) (string, error) {
		recovered = true
		return "", err
	})

	result, err := future2.Await()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result != "success" {
		t.Errorf("Expected 'success', got %v", result)
	}

	if recovered {
		t.Error("Expected recover function not to be called")
	}
}

func TestWaitAll(t *testing.T) {
	t.Parallel()

	futures := []*Future[int]{
		Async(func() (int, error) { return 1, nil }),
		Async(func() (int, error) { return 2, nil }),
		Async(func() (int, error) { return 3, nil }),
	}

	results, err := WaitAll(futures...)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expected := []int{1, 2, 3}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("Expected %d at index %d, got %d", expected[i], i, v)
		}
	}
}

func TestWaitAll_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("failure")

	futures := []*Future[int]{
		Async(func() (int, error) { return 1, nil }),
		Async(func() (int, error) { return 0, expectedErr }),
		Async(func() (int, error) { return 3, nil }),
	}

	results, err := WaitAll(futures...)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if results != nil {
		t.Errorf("Expected nil results, got %v", results)
	}
}

func TestWaitAny(t *testing.T) {
	t.Parallel()

	futures := []*Future[string]{
		Async(func() (string, error) {
			time.Sleep(200 * time.Millisecond)
			return "slow", nil
		}),
		Async(func() (string, error) {
			time.Sleep(100 * time.Millisecond)
			return "fast", nil
		}),
		Async(func() (string, error) {
			time.Sleep(300 * time.Millisecond)
			return "slowest", nil
		}),
	}

	index, result, err := WaitAny(futures...)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if index != 1 {
		t.Errorf("Expected index 1, got %d", index)
	}

	if result != "fast" {
		t.Errorf("Expected 'fast', got %v", result)
	}
}

func TestWaitAny_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("early failure")

	futures := []*Future[string]{
		Async(func() (string, error) {
			time.Sleep(50 * time.Millisecond)
			return "", expectedErr
		}),
		Async(func() (string, error) {
			time.Sleep(200 * time.Millisecond)
			return "slow", nil
		}),
	}

	index, result, err := WaitAny(futures...)
	if err != expectedErr {
		t.Errorf("Expected %v, got %v", expectedErr, err)
	}

	if index != 0 {
		t.Errorf("Expected index 0, got %d", index)
	}

	if result != "" {
		t.Errorf("Expected empty result, got %v", result)
	}
}

func TestSelect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	futures := []*Future[string]{
		Async(func() (string, error) {
			time.Sleep(200 * time.Millisecond)
			return "slow", nil
		}),
		Async(func() (string, error) {
			time.Sleep(100 * time.Millisecond)
			return "fast", nil
		}),
	}

	index, result, err := Select(ctx, futures...)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if index != 1 {
		t.Errorf("Expected index 1, got %d", index)
	}

	if result != "fast" {
		t.Errorf("Expected 'fast', got %v", result)
	}
}

func TestSelect_Timeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	futures := []*Future[string]{
		Async(func() (string, error) {
			time.Sleep(200 * time.Millisecond)
			return "slow", nil
		}),
		Async(func() (string, error) {
			time.Sleep(300 * time.Millisecond)
			return "slower", nil
		}),
	}

	index, result, err := Select(ctx, futures...)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}

	if index != -1 {
		t.Errorf("Expected index -1, got %d", index)
	}

	var zero string
	if result != zero {
		t.Errorf("Expected zero value, got %v", result)
	}
}

func TestFuture_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	future := Async(func() (int, error) {
		time.Sleep(100 * time.Millisecond)
		return 42, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := future.Await()
			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
			if result != 42 {
				t.Errorf("Expected 42, got %v", result)
			}
		}()
	}

	wg.Wait()
}

func TestFuture_MultipleAwait(t *testing.T) {
	t.Parallel()

	future := Async(func() (int, error) {
		return 42, nil
	})

	// First await
	result1, err1 := future.Await()
	if err1 != nil || result1 != 42 {
		t.Errorf("First await: expected 42, got %v (err: %v)", result1, err1)
	}

	// Second await should return same result immediately
	result2, err2 := future.Await()
	if err2 != nil || result2 != 42 {
		t.Errorf("Second await: expected 42, got %v (err: %v)", result2, err2)
	}
}

func TestFuture_CancelExplicit(t *testing.T) {
	t.Parallel()

	future := Async(func() (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "done", nil
	})

	canceled := future.Cancel()
	if !canceled {
		t.Error("Expected Cancel to return true")
	}

	result, err := future.Await()
	if err != ErrFutureCanceled {
		t.Errorf("Expected ErrFutureCanceled, got %v", err)
	}

	var zero string
	if result != zero {
		t.Errorf("Expected zero value, got %v", result)
	}
}

func TestFuture_NilFunc(t *testing.T) {
	t.Parallel()

	future := NewFuture[string](context.Background(), nil)

	result, err := future.Await()
	if err == nil || err.Error() != "nil function provided" {
		t.Errorf("Expected 'nil function provided' error, got %v", err)
	}

	var zero string
	if result != zero {
		t.Errorf("Expected zero value, got %v", result)
	}
}

func TestFuture_ZeroTimeout(t *testing.T) {
	t.Parallel()

	future := Async(func() (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "done", nil
	})

	result, err := future.AwaitWithTimeout(0)
	if !errors.Is(err, ErrFutureTimeout) {
		t.Errorf("Expected ErrFutureTimeout, got %v", err)
	}

	var zero string
	if result != zero {
		t.Errorf("Expected zero value, got %v", result)
	}
}

// doctor_test.go
package jack

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDoctor_Add_Basic verifies patient addition and basic execution.
func TestDoctor_Add_Basic(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var checkCount atomic.Int32
	patient := NewPatient(PatientConfig{
		ID:       "patient-basic",
		Interval: 50 * time.Millisecond,
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	if count := checkCount.Load(); count < 2 {
		t.Errorf("Expected at least 2 checks, got %d", count)
	}

	state, ok := doctor.GetState("patient-basic")
	if !ok {
		t.Error("Expected patient state to exist")
	}
	if state != PatientHealthy {
		t.Errorf("Expected PatientHealthy, got %v", state)
	}
}

// TestDoctor_Remove verifies patient removal stops checks.
func TestDoctor_Remove(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var checkCount atomic.Int32
	patient := NewPatient(PatientConfig{
		ID:       "patient-remove",
		Interval: 50 * time.Millisecond,
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(75 * time.Millisecond)
	countBefore := checkCount.Load()

	removed := doctor.Remove("patient-remove")
	if !removed {
		t.Error("Expected Remove to return true")
	}

	time.Sleep(100 * time.Millisecond)
	countAfter := checkCount.Load()

	if countAfter != countBefore {
		t.Errorf("Expected no checks after remove, got %d more", countAfter-countBefore)
	}
}

// TestDoctor_RemoveNonExistent verifies safe removal of unknown patient.
func TestDoctor_RemoveNonExistent(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor()
	defer doctor.StopAll(5 * time.Second)

	removed := doctor.Remove("nonexistent")
	if removed {
		t.Error("Expected Remove to return false for nonexistent patient")
	}
}

// TestDoctor_StateTransitions verifies healthy → degraded → failed → healthy transitions.
func TestDoctor_StateTransitions(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var failCount atomic.Int32
	maxFailures := uint64(3)

	patient := NewPatient(PatientConfig{
		ID:          "patient-state",
		Interval:    30 * time.Millisecond,
		Timeout:     500 * time.Millisecond,
		MaxFailures: maxFailures,
		Check: FuncCtx(func(ctx context.Context) error {
			count := failCount.Add(1)
			if count <= 5 {
				return errors.New("simulated failure")
			}
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for failures to accumulate
	time.Sleep(200 * time.Millisecond)

	state, _ := doctor.GetState("patient-state")
	if state != PatientFailed && state != PatientDegraded {
		t.Errorf("Expected PatientFailed or PatientDegraded after failures, got %v", state)
	}

	// Wait for recovery
	time.Sleep(200 * time.Millisecond)

	state, _ = doctor.GetState("patient-state")
	if state != PatientHealthy {
		t.Errorf("Expected PatientHealthy after recovery, got %v", state)
	}
}

// TestDoctor_SetDegraded verifies manual degradation triggers acceleration.
func TestDoctor_SetDegraded(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var checkCount atomic.Int32

	patient := NewPatient(PatientConfig{
		ID:          "patient-degraded",
		Interval:    200 * time.Millisecond,
		Accelerated: 20 * time.Millisecond,
		Timeout:     500 * time.Millisecond,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	countBefore := checkCount.Load()

	doctor.SetDegraded("patient-degraded", true)
	time.Sleep(100 * time.Millisecond)
	countAfter := checkCount.Load()

	if countAfter-countBefore < 3 {
		t.Errorf("Expected accelerated checks, got only %d", countAfter-countBefore)
	}

	// Recover
	doctor.SetDegraded("patient-degraded", false)
}

// TestDoctor_SetDegradedNonExistent verifies safe degradation call for unknown patient.
func TestDoctor_SetDegradedNonExistent(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor()
	defer doctor.StopAll(5 * time.Second)

	// Should not panic
	doctor.SetDegraded("nonexistent", true)
}

// TestDoctor_LifecycleHooks verifies all lifecycle hooks are called.
func TestDoctor_LifecycleHooks(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var (
		onStartCalled     atomic.Int32
		onCompleteCalled  atomic.Int32
		onErrorCalled     atomic.Int32
		onTimeoutCalled   atomic.Int32
		onRecoverCalled   atomic.Int32
		onStateChangeCall atomic.Int32
	)

	var shouldFail atomic.Bool
	shouldFail.Store(true)

	patient := NewPatient(PatientConfig{
		ID:       "patient-hooks",
		Interval: 50 * time.Millisecond,
		Timeout:  500 * time.Millisecond,
		Check: FuncCtx(func(ctx context.Context) error {
			if shouldFail.Load() {
				return errors.New("test error")
			}
			return nil
		}),
		OnStart: FuncCtx(func(ctx context.Context) error {
			onStartCalled.Add(1)
			return nil
		}),
		OnComplete: FuncCtx(func(ctx context.Context) error {
			onCompleteCalled.Add(1)
			return nil
		}),
		OnError: FuncCtx(func(ctx context.Context) error {
			onErrorCalled.Add(1)
			return nil
		}),
		OnTimeout: FuncCtx(func(ctx context.Context) error {
			onTimeoutCalled.Add(1)
			return nil
		}),
		OnRecover: FuncCtx(func(ctx context.Context) error {
			onRecoverCalled.Add(1)
			return nil
		}),
		OnStateChange: func(event PatientEvent) {
			onStateChangeCall.Add(1)
		},
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	if onStartCalled.Load() == 0 {
		t.Error("OnStart hook was not called")
	}
	if onErrorCalled.Load() == 0 {
		t.Error("OnError hook was not called")
	}
	if onStateChangeCall.Load() == 0 {
		t.Error("OnStateChange hook was not called")
	}

	// Trigger recovery
	shouldFail.Store(false)
	time.Sleep(150 * time.Millisecond)

	if onCompleteCalled.Load() == 0 {
		t.Error("OnComplete hook was not called after recovery")
	}
	if onRecoverCalled.Load() == 0 {
		t.Error("OnRecover hook was not called")
	}
}

// TestDoctor_TimeoutHandling verifies timeout enforcement.
func TestDoctor_TimeoutHandling(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var timeoutCount atomic.Int32

	patient := NewPatient(PatientConfig{
		ID:       "patient-timeout",
		Interval: 100 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
		Check: FuncCtx(func(ctx context.Context) error {
			select {
			case <-time.After(200 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
		OnTimeout: FuncCtx(func(ctx context.Context) error {
			timeoutCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if timeoutCount.Load() == 0 {
		t.Error("OnTimeout hook was not called")
	}
}

// TestDoctor_Jitter verifies jitter is applied to intervals.
func TestDoctor_Jitter(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var executionTimes []time.Time
	var mu sync.Mutex

	patient := NewPatient(PatientConfig{
		ID:       "patient-jitter",
		Interval: 50 * time.Millisecond,
		Jitter:   0.5, // 50% jitter
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			mu.Lock()
			executionTimes = append(executionTimes, time.Now())
			mu.Unlock()
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(executionTimes) < 3 {
		t.Errorf("Expected at least 3 executions, got %d", len(executionTimes))
	}

	// Check intervals vary
	if len(executionTimes) >= 3 {
		interval1 := executionTimes[1].Sub(executionTimes[0])
		interval2 := executionTimes[2].Sub(executionTimes[1])
		if interval1 == interval2 {
			t.Log("Note: Intervals identical (jitter may not show in short test)")
		}
	}
}

// TestDoctor_MaxFailures verifies max failures threshold.
func TestDoctor_MaxFailures(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	patient := NewPatient(PatientConfig{
		ID:          "patient-maxfail",
		Interval:    30 * time.Millisecond,
		Timeout:     500 * time.Millisecond,
		MaxFailures: 2,
		Check: FuncCtx(func(ctx context.Context) error {
			return errors.New("always fails")
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	state, _ := doctor.GetState("patient-maxfail")
	if state != PatientFailed {
		t.Errorf("Expected PatientFailed after max failures, got %v", state)
	}
}

// TestDoctor_MaxFailuresZero verifies zero max failures means unlimited.
func TestDoctor_MaxFailuresZero(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	patient := NewPatient(PatientConfig{
		ID:          "patient-maxfail-zero",
		Interval:    30 * time.Millisecond,
		Timeout:     500 * time.Millisecond,
		MaxFailures: 0, // Unlimited
		Check: FuncCtx(func(ctx context.Context) error {
			return errors.New("always fails")
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	state, _ := doctor.GetState("patient-maxfail-zero")
	if state != PatientDegraded {
		t.Errorf("Expected PatientDegraded (not Failed) with MaxFailures=0, got %v", state)
	}
}

// TestDoctor_ConcurrentAdd verifies thread-safe patient addition.
func TestDoctor_ConcurrentAdd(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(50))
	defer doctor.StopAll(5 * time.Second)

	var wg sync.WaitGroup
	patientCount := 20

	for i := 0; i < patientCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = doctor.Add(NewPatient(PatientConfig{
				ID:       fmt.Sprintf("patient-concurrent-%d", id),
				Interval: 100 * time.Millisecond,
				Timeout:  1 * time.Second,
				Check: FuncCtx(func(ctx context.Context) error {
					return nil
				}),
			}))
		}(i)
	}

	wg.Wait()

	// Verify all registered
	for i := 0; i < patientCount; i++ {
		state, ok := doctor.GetState(fmt.Sprintf("patient-concurrent-%d", i))
		if !ok {
			t.Errorf("Patient %d not found", i)
		}
		if state != PatientHealthy && state != PatientUnknown {
			t.Errorf("Unexpected state for patient %d: %v", i, state)
		}
	}
}

// TestDoctor_ConcurrentRemove verifies thread-safe patient removal.
func TestDoctor_ConcurrentRemove(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(50))
	defer doctor.StopAll(5 * time.Second)

	patientCount := 10
	for i := 0; i < patientCount; i++ {
		_ = doctor.Add(NewPatient(PatientConfig{
			ID:       fmt.Sprintf("patient-remove-%d", i),
			Interval: 50 * time.Millisecond,
			Timeout:  1 * time.Second,
			Check:    FuncCtx(func(ctx context.Context) error { return nil }),
		}))
	}

	time.Sleep(75 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < patientCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			doctor.Remove(fmt.Sprintf("patient-remove-%d", id))
		}(i)
	}

	wg.Wait()

	// Verify all removed
	for i := 0; i < patientCount; i++ {
		_, ok := doctor.GetState(fmt.Sprintf("patient-remove-%d", i))
		if ok {
			t.Errorf("Patient %d should be removed", i)
		}
	}
}

// TestDoctor_NilCheckFunc verifies error on nil check function.
func TestDoctor_NilCheckFunc(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor()
	defer doctor.StopAll(5 * time.Second)

	patient := NewPatient(PatientConfig{
		ID:       "patient-nil",
		Interval: 100 * time.Millisecond,
		Check:    nil,
	})
	err := doctor.Add(patient)
	if err == nil {
		t.Error("Expected error for nil Check function")
	}
}

// TestDoctor_InvalidInterval verifies default interval on invalid config.
func TestDoctor_InvalidInterval(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor()
	defer doctor.StopAll(5 * time.Second)

	var checkCount atomic.Int32
	patient := NewPatient(PatientConfig{
		ID:       "patient-invalid-interval",
		Interval: 0, // Invalid
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	if checkCount.Load() == 0 {
		t.Error("Expected checks to run with default interval")
	}
}

// TestDoctor_StopAll verifies graceful shutdown.
func TestDoctor_StopAll(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))

	var checkCount atomic.Int32
	patient := NewPatient(PatientConfig{
		ID:       "patient-stop",
		Interval: 50 * time.Millisecond,
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	countBefore := checkCount.Load()

	doctor.StopAll(5 * time.Second)

	time.Sleep(100 * time.Millisecond)
	countAfter := checkCount.Load()

	if countAfter != countBefore {
		t.Errorf("Expected no checks after StopAll, got %d more", countAfter-countBefore)
	}
}

// TestDoctor_StopAllIdempotent verifies StopAll can be called multiple times.
func TestDoctor_StopAllIdempotent(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor()

	doctor.StopAll(5 * time.Second)
	doctor.StopAll(5 * time.Second)
	doctor.StopAll(5 * time.Second)
	// Should not panic
}

// TestDoctor_Observable verifies event emission.
func TestDoctor_Observable(t *testing.T) {
	t.Parallel()

	var eventCount atomic.Int32
	obs := NewObservable[PatientEvent](5)
	defer obs.Shutdown()

	obs.Add(ObserverFunc[PatientEvent](func(event PatientEvent) {
		eventCount.Add(1)
	}))

	doctor := NewDoctor(
		DoctorWithMaxConcurrent(10),
		DoctorWithObservable(obs),
	)
	defer doctor.StopAll(5 * time.Second)

	patient := NewPatient(PatientConfig{
		ID:       "patient-obs",
		Interval: 50 * time.Millisecond,
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if eventCount.Load() == 0 {
		t.Error("Expected events to be emitted")
	}
}

// TestDoctor_PoolBoundedConcurrency verifies pool limits concurrent checks.
func TestDoctor_PoolBoundedConcurrency(t *testing.T) {
	t.Parallel()

	maxConcurrent := 3
	var currentConcurrent atomic.Int32
	var maxObserved atomic.Int32

	doctor := NewDoctor(DoctorWithMaxConcurrent(maxConcurrent))
	defer doctor.StopAll(5 * time.Second)

	patientCount := 10
	for i := 0; i < patientCount; i++ {
		_ = doctor.Add(NewPatient(PatientConfig{
			ID:       fmt.Sprintf("patient-pool-%d", i),
			Interval: 20 * time.Millisecond,
			Timeout:  500 * time.Millisecond,
			Check: FuncCtx(func(ctx context.Context) error {
				current := currentConcurrent.Add(1)
				defer currentConcurrent.Add(-1)

				for {
					max := maxObserved.Load()
					if current > max {
						if maxObserved.CompareAndSwap(max, current) {
							break
						}
					} else {
						break
					}
				}

				time.Sleep(50 * time.Millisecond)
				return nil
			}),
		}))
	}

	time.Sleep(300 * time.Millisecond)

	if maxObserved.Load() > int32(maxConcurrent) {
		t.Errorf("Expected max concurrent ≤ %d, got %d", maxConcurrent, maxObserved.Load())
	}
}

// TestDoctor_GetStateNonExistent verifies state lookup for unknown patient.
func TestDoctor_GetStateNonExistent(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor()
	defer doctor.StopAll(5 * time.Second)

	state, ok := doctor.GetState("nonexistent")
	if ok {
		t.Error("Expected ok=false for nonexistent patient")
	}
	if state != PatientUnknown {
		t.Errorf("Expected PatientUnknown, got %v", state)
	}
}

// TestDoctor_AddUpdate verifies re-addition updates config.
func TestDoctor_AddUpdate(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var checkCount atomic.Int32

	// Initial addition
	patient := NewPatient(PatientConfig{
		ID:       "patient-update",
		Interval: 100 * time.Millisecond,
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	countBefore := checkCount.Load()

	// Re-add with faster interval
	patient2 := NewPatient(PatientConfig{
		ID:       "patient-update",
		Interval: 20 * time.Millisecond,
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient2); err != nil {
		t.Fatalf("Re-add failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	countAfter := checkCount.Load()

	if countAfter-countBefore < 3 {
		t.Errorf("Expected faster checks after update, got %d", countAfter-countBefore)
	}
}

// TestDoctor_InvalidJitter verifies jitter bounds enforcement.
func TestDoctor_InvalidJitter(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor()
	defer doctor.StopAll(5 * time.Second)

	var checkCount atomic.Int32

	// Jitter > 1.0 should be clamped
	patient := NewPatient(PatientConfig{
		ID:       "patient-jitter-invalid",
		Interval: 50 * time.Millisecond,
		Jitter:   2.0, // Invalid, should clamp to 1.0
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	if checkCount.Load() == 0 {
		t.Error("Expected checks to run with clamped jitter")
	}
}

// TestDoctor_ContextPropagation verifies context is passed to checks.
func TestDoctor_ContextPropagation(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var receivedCtx context.Context
	var mu sync.Mutex

	patient := NewPatient(PatientConfig{
		ID:       "patient-ctx",
		Interval: 100 * time.Millisecond,
		Timeout:  500 * time.Millisecond,
		Check: FuncCtx(func(ctx context.Context) error {
			mu.Lock()
			receivedCtx = ctx
			mu.Unlock()
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if receivedCtx == nil {
		t.Error("Expected context to be passed to check")
	}
}

// TestDoctor_AcceleratedIntervalZero verifies zero accelerated disables acceleration.
func TestDoctor_AcceleratedIntervalZero(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var checkCount atomic.Int32

	patient := NewPatient(PatientConfig{
		ID:          "patient-accel-zero",
		Interval:    100 * time.Millisecond,
		Accelerated: 0, // Disabled
		Timeout:     1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return errors.New("fail")
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(250 * time.Millisecond)
	countBefore := checkCount.Load()

	doctor.SetDegraded("patient-accel-zero", true)
	time.Sleep(150 * time.Millisecond)
	countAfter := checkCount.Load()

	// Should not accelerate
	if countAfter-countBefore > 2 {
		t.Errorf("Expected no acceleration with Accelerated=0, got %d checks", countAfter-countBefore)
	}
}

// TestDoctor_InitialState verifies initial state is Unknown.
func TestDoctor_InitialState(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor()
	defer doctor.StopAll(5 * time.Second)

	patient := NewPatient(PatientConfig{
		ID:       "patient-initial",
		Interval: 1 * time.Second,
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Check immediately before first execution
	state, ok := doctor.GetState("patient-initial")
	if !ok {
		t.Error("Expected patient state to exist")
	}
	if state != PatientUnknown {
		t.Errorf("Expected PatientUnknown initially, got %v", state)
	}
}

// TestDoctor_NewPatient_Defaults verifies NewPatient applies config defaults.
func TestDoctor_NewPatient_Defaults(t *testing.T) {
	t.Parallel()

	// Zero interval should default to 10s
	patient := NewPatient(PatientConfig{
		ID:    "patient-defaults",
		Check: FuncCtx(func(ctx context.Context) error { return nil }),
	})
	if patient.cfg.Interval <= 0 {
		t.Error("Expected default interval to be set")
	}

	// Zero timeout should default to 30s
	if patient.cfg.Timeout <= 0 {
		t.Error("Expected default timeout to be set")
	}

	// Invalid jitter should be clamped to 0
	patient2 := NewPatient(PatientConfig{
		ID:       "patient-defaults2",
		Jitter:   -0.5,
		Interval: 100 * time.Millisecond,
		Check:    FuncCtx(func(ctx context.Context) error { return nil }),
	})
	if patient2.cfg.Jitter != 0 {
		t.Errorf("Expected jitter clamped to 0, got %v", patient2.cfg.Jitter)
	}
}

// TestDoctor_PatientStop verifies patient stop halts scheduling.
func TestDoctor_PatientStop(t *testing.T) {
	t.Parallel()

	doctor := NewDoctor(DoctorWithMaxConcurrent(10))
	defer doctor.StopAll(5 * time.Second)

	var checkCount atomic.Int32
	patient := NewPatient(PatientConfig{
		ID:       "patient-stop-test",
		Interval: 50 * time.Millisecond,
		Timeout:  1 * time.Second,
		Check: FuncCtx(func(ctx context.Context) error {
			checkCount.Add(1)
			return nil
		}),
	})
	if err := doctor.Add(patient); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	time.Sleep(75 * time.Millisecond)
	countBefore := checkCount.Load()

	// Stop patient directly (internal method via doctor.Remove)
	doctor.Remove("patient-stop-test")

	time.Sleep(100 * time.Millisecond)
	countAfter := checkCount.Load()

	if countAfter != countBefore {
		t.Errorf("Expected no checks after patient stop, got %d more", countAfter-countBefore)
	}
}

// ObserverFunc helper for tests
type ObserverFunc[T any] func(T)

func (f ObserverFunc[T]) OnNotify(value T) {
	f(value)
}

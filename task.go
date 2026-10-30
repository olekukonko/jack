package jack

import (
	"context"
	"errors"
	"sync"
)

// tasker wraps a Task or TaskCtx with a context and ID generation logic.
// It implements the job interface for execution in the worker pool.
type tasker struct {
	task            interface{}
	ctx             context.Context
	taskIDGenerator func(interface{}) string
	defaultIDPrefix string
	noID            bool // when true, skip ID generation entirely

	// idOnce caches the generated ID. ID() is called by both the submitter
	// (for the "queued" event) and the worker (for "run"/"done"); regenerating
	// produced a different ULID each call, so events for one task could not be
	// correlated and every task paid for two ULIDs.
	idOnce sync.Once
	id     string
}

// Context returns the task's associated context.
func (tj *tasker) Context() context.Context {
	if tj.ctx == nil {
		return context.Background()
	}
	return tj.ctx
}

// ID returns a stable identifier for the task, generating it on first use.
// Priority: taskIDGenerator → Identifiable.ID → ULID (globally unique).
// When noID is true, returns "" immediately with zero allocation.
// The value is cached so every caller (submitter and worker) observes the same ID.
func (tj *tasker) ID() string {
	tj.idOnce.Do(func() { tj.id = tj.generateID() })
	return tj.id
}

// generateID produces the identifier on first use; see ID.
func (tj *tasker) generateID() string {
	if tj.noID {
		return ""
	}
	if tj.taskIDGenerator != nil {
		if id := tj.taskIDGenerator(tj.task); id != "" {
			return id
		}
	}
	if identifiable, ok := tj.task.(Identifiable); ok {
		if id := identifiable.ID(); id != "" {
			return id
		}
	}
	if tj.task == nil {
		return tj.defaultIDPrefix + ".nil_task." + newULID()
	}
	return tj.defaultIDPrefix + "." + newULID()
}

// Run executes the wrapped Task or TaskCtx using the stored context.
func (tj *tasker) Run(_ context.Context) error {
	if taskCtx, ok := tj.task.(TaskCtx); ok {
		return taskCtx.Do(tj.ctx)
	}
	if task, ok := tj.task.(Task); ok {
		return task.Do()
	}
	return errors.New("invalid task type")
}

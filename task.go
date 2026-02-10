package jack

import (
	"context"
	"errors"

	"strings"

	"github.com/oklog/ulid/v2"
)

// tasker wraps a Task or TaskCtx with a context and ID generation logic.
// It implements the job interface for execution in the worker pool.
// Thread-safe via context and immutable fields.
type tasker struct {
	task            interface{}              // Task or TaskCtx to execute
	ctx             context.Context          // Context for TaskCtx or Background for Task
	taskIDGenerator func(interface{}) string // Optional ID generator
	defaultIDPrefix string                   // Prefix for generated IDs
}

// Context returns the task's associated context, defaulting to context.Background if none is set.
func (tj *tasker) Context() context.Context {
	if tj.ctx == nil {
		return context.Background()
	}
	return tj.ctx
}

// ID generates a unique identifier for the task.
// It uses the taskIDGenerator, Identifiable.ID, or a generated ID with the default prefix.
func (tj *tasker) ID() string {
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
		return strings.Join([]string{tj.defaultIDPrefix, "nil_task", ulid.Make().String()}, ".")
	}
	return strings.Join([]string{tj.defaultIDPrefix, ulid.Make().String()}, ".")
}

// Run executes the wrapped Task or TaskCtx using the stored context.
// It returns an error if the task type is invalid.
func (tj *tasker) Run(_ context.Context) error {
	if taskCtx, ok := tj.task.(TaskCtx); ok {
		return taskCtx.Do(tj.ctx)
	}
	if task, ok := tj.task.(Task); ok {
		return task.Do()
	}
	return errors.New("invalid task type")
}

package jack

import (
	"reflect"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// defaultIDRunner generates a unique ID for a runner task.
// It uses the "runner" prefix and the task’s Identifiable ID or a ULID.
// Thread-safe as it relies on ULID generation and interface checks.
func defaultIDRunner(input interface{}) string {
	return defaultID("runner", input)
}

// defaultIDTask generates a unique ID for a task.
// It uses the "task" prefix and the task’s Identifiable ID or a ULID.
// Thread-safe as it relies on ULID generation and interface checks.
func defaultIDTask(input interface{}) string {
	return defaultID("task", input)
}

// defaultIDScheduler generates a unique ID for a scheduler task.
// It uses the "scheduler" prefix and the task’s Identifiable ID or a ULID.
// Thread-safe as it relies on ULID generation and interface checks.
func defaultIDScheduler(input interface{}) string {
	return defaultID("scheduler", input)
}

// defaultID generates a unique ID for a task with the given name prefix.
// It uses the task’s Identifiable ID if available, returns "nil_task" for nil input,
// or generates a ULID with the prefix.
// Thread-safe as it relies on ULID generation and interface checks.
func defaultID(name string, taskInput interface{}) string {
	if identifiable, ok := taskInput.(Identifiable); ok {
		return identifiable.ID()
	}
	if taskInput == nil {
		return "nil_task"
	}
	return strings.Join([]string{name, ulid.Make().String()}, ".")
}

// typeName returns a simplified string representation of the input’s type.
// It handles pointers, generic types, and module paths, extracting the base type name.
// Thread-safe as it uses reflection without shared state.
func typeName(v interface{}) string {
	t := reflect.TypeOf(v)
	if t == nil {
		return "nil"
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if name := t.Name(); name != "" && !strings.Contains(name, "[") {
		return name
	}
	typeStr := t.String()
	if bracketStart := strings.Index(typeStr, "["); bracketStart > 0 {
		bracketEnd := strings.LastIndex(typeStr, "]")
		if bracketEnd > bracketStart {
			innerType := typeStr[bracketStart+1 : bracketEnd]
			if lastSlash := strings.LastIndex(innerType, "/"); lastSlash >= 0 {
				innerType = innerType[lastSlash+1:]
			}
			innerType = strings.Split(innerType, "]")[0]
			return innerType
		}
	}
	parts := strings.Split(typeStr, ".")
	lastPart := parts[len(parts)-1]
	if idx := strings.IndexAny(lastPart, "[]"); idx != -1 {
		lastPart = lastPart[:idx]
	}
	return lastPart
}

// stopAndDrainTimer safely stops a timer and drains its channel if necessary.
func stopAndDrainTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

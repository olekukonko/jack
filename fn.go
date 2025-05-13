package jack

import (
	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/ll"
	"reflect"
	"strings"
)

func Logger() *ll.Logger {
	return logger
}

func defaultIDRunner(input interface{}) string {
	return defaultID("runner", input)
}

func defaultIDTask(input interface{}) string {
	return defaultID("task", input)
}

func defaultIDScheduler(input interface{}) string {
	return defaultID("scheduler", input)
}

func defaultID(name string, taskInput interface{}) string {
	if identifiable, ok := taskInput.(Identifiable); ok {
		return identifiable.ID()
	}
	if taskInput == nil {
		return "nil_task"
	}
	return strings.Join([]string{name, ulid.Make().String()}, ".")
}

func typeName(v interface{}) string {

	t := reflect.TypeOf(v)
	if t == nil {
		return "nil"
	}

	// Handle pointers
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// First try to get the simple name
	if name := t.Name(); name != "" && !strings.Contains(name, "[") {
		return name
	}

	// For complex types, parse the string representation
	typeStr := t.String()

	// Special handling for generic/adapter types
	if bracketStart := strings.Index(typeStr, "["); bracketStart > 0 {
		// Extract content inside brackets
		bracketEnd := strings.LastIndex(typeStr, "]")
		if bracketEnd > bracketStart {
			innerType := typeStr[bracketStart+1 : bracketEnd]

			// Extract the last part after slash
			if lastSlash := strings.LastIndex(innerType, "/"); lastSlash >= 0 {
				innerType = innerType[lastSlash+1:]
			}

			// Remove any remaining brackets (for nested cases)
			innerType = strings.Split(innerType, "]")[0]
			return innerType
		}
	}

	// Fallback for non-generic types
	parts := strings.Split(typeStr, ".")
	lastPart := parts[len(parts)-1]
	if idx := strings.IndexAny(lastPart, "[]"); idx != -1 {
		lastPart = lastPart[:idx]
	}
	return lastPart
}

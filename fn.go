package jack

import (
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// randReader wraps math/rand/v2 to implement io.Reader for ulid.Monotonic.
// math/rand/v2 removed the Read method; this shim restores it with zero overhead.
type randReader struct{ r *rand.Rand }

// Read fills p with pseudo-random bytes from the underlying PCG generator.
// This shim satisfies io.Reader for ulid.Monotonic without using crypto/rand.
func (rr *randReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(rr.r.Uint32())
	}
	return len(p), nil
}

// ulidEntropy is a package-level monotonic ULID entropy source backed by
// math/rand/v2 PCG — non-blocking, no syscall, goroutine-safe via ulidMu.
// Monotonic ensures IDs within the same millisecond are strictly ordered.
var (
	ulidMu      sync.Mutex
	ulidEntropy = ulid.Monotonic(&randReader{rand.New(rand.NewPCG(
		uint64(time.Now().UnixNano()),
		uint64(time.Now().UnixNano()>>32),
	))}, 0)
)

// newULID returns a globally-unique, time-sortable ULID string.
// Safe for concurrent use; the mutex is held only for the duration of the
// ulid.MustNew call, which is a handful of nanoseconds.
func newULID() string {
	ulidMu.Lock()
	id := ulid.MustNew(ulid.Now(), ulidEntropy)
	ulidMu.Unlock()
	return id.String()
}

// defaultIDRunner generates a unique ID for a runner task.
func defaultIDRunner(input interface{}) string { return defaultID("runner", input) }

// defaultIDTask generates a unique ID for a pool task.
func defaultIDTask(input interface{}) string { return defaultID("task", input) }

// defaultIDScheduler generates a unique ID for a scheduler task.
func defaultIDScheduler(input interface{}) string { return defaultID("scheduler", input) }

// defaultID generates a unique ID for a task with the given name prefix.
// Identifiable tasks supply their own ID; others get a ULID for global uniqueness
// across distributed processes without collision.
func defaultID(name string, taskInput interface{}) string {
	if identifiable, ok := taskInput.(Identifiable); ok {
		return identifiable.ID()
	}
	if taskInput == nil {
		return "nil_task"
	}
	return name + "." + newULID()
}

// typeName returns a simplified string representation of the input's type.
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

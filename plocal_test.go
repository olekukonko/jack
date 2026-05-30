package jack

import (
	"sync"
	"testing"
)

func TestPLocalCounter(t *testing.T) {
	var c PLocalCounter
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Add(1)
			}
		}()
	}
	wg.Wait()
	if v := c.Value(); v != 100000 {
		t.Fatalf("expected 100000, got %d", v)
	}
}

func TestPLocal(t *testing.T) {
	var p PLocal[int]
	p.Set(42)
	if v := p.Get(); v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}

	p.With(func(v *int) {
		*v = 100
	})
	if v := p.Get(); v != 100 {
		t.Fatalf("expected 100, got %d", v)
	}
}

func TestPLocal_Concurrent(t *testing.T) {
	var p PLocal[int]
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.With(func(v *int) {
				*v++
			})
		}()
	}
	wg.Wait()
	if p.Fold(0, func(a, b int) int { return a + b }) < 1 {
		t.Fatal("expected positive value")
	}
}

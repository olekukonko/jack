package jack

import (
	"context"
	"time"
)

type Vitals struct {
	Start     HookCtx
	End       CallbackCtx
	Timed     CallbackCtx
	TimedWait time.Duration
}

type VitalsOption func(*Vitals)

func VitalsWithStart(hook HookCtx) VitalsOption {
	return func(l *Vitals) { l.Start = hook }
}

func VitalsWithEnd(callback CallbackCtx) VitalsOption {
	return func(l *Vitals) { l.End = callback }
}

func VitalsWithTimed(callback CallbackCtx, wait time.Duration) VitalsOption {
	return func(l *Vitals) {
		l.Timed = callback
		l.TimedWait = wait
	}
}

func NewVitals(opts ...VitalsOption) *Vitals {
	l := &Vitals{}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (l *Vitals) Execute(ctx context.Context, id string, operation Func) error {
	if l.Start != nil {
		if err := l.Start(ctx, id); err != nil {
			return err
		}
	}
	if err := operation(); err != nil {
		return err
	}
	if l.End != nil {
		l.End(ctx, id)
	}
	return nil
}

func (l *Vitals) ExecuteCtx(ctx context.Context, id string, operation FuncCtx) error {
	if l.Start != nil {
		if err := l.Start(ctx, id); err != nil {
			return err
		}
	}
	if err := operation(ctx); err != nil {
		return err
	}
	if l.End != nil {
		l.End(ctx, id)
	}
	return nil
}

func VitalsWithRun(ctx context.Context, id string, operation Func, opts ...VitalsOption) error {
	lifetime := NewVitals(opts...)
	return lifetime.Execute(ctx, id, operation)
}

func VitalsWithRunCtx(ctx context.Context, id string, operation FuncCtx, opts ...VitalsOption) error {
	lifetime := NewVitals(opts...)
	return lifetime.ExecuteCtx(ctx, id, operation)
}

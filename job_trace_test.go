package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

type testTraceHook struct {
	startCalls   int
	successCalls int
	failureCalls int
	lastName     string
	lastErr      error
	lastDuration time.Duration
}

func (h *testTraceHook) Start(ctx context.Context, name string) context.Context {
	h.startCalls++
	h.lastName = name
	return context.WithValue(ctx, "trace-started", true)
}

func (h *testTraceHook) Success(ctx context.Context, duration time.Duration) {
	h.successCalls++
	h.lastDuration = duration
}

func (h *testTraceHook) Failure(ctx context.Context, err error, duration time.Duration) {
	h.failureCalls++
	h.lastErr = err
	h.lastDuration = duration
}

func TestWithTracing(t *testing.T) {
	t.Run("nil_hook_passthrough", func(t *testing.T) {
		called := false
		job := WithTracing(func(ctx context.Context) error {
			called = true
			return nil
		}, "no-hook", nil)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithTracing(): expected nil error, got: %v", err)
		}
		if !called {
			t.Fatal("WithTracing(): expected wrapped job to run")
		}
	})

	t.Run("success_path", func(t *testing.T) {
		hook := &testTraceHook{}
		ctxSeen := false

		job := WithTracing(func(ctx context.Context) error {
			if v, ok := ctx.Value("trace-started").(bool); ok && v {
				ctxSeen = true
			}
			return nil
		}, "success-job", hook)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithTracing(): expected nil error, got: %v", err)
		}
		if hook.startCalls != 1 {
			t.Fatalf("WithTracing(): startCalls=%d, want=1", hook.startCalls)
		}
		if hook.successCalls != 1 {
			t.Fatalf("WithTracing(): successCalls=%d, want=1", hook.successCalls)
		}
		if hook.failureCalls != 0 {
			t.Fatalf("WithTracing(): failureCalls=%d, want=0", hook.failureCalls)
		}
		if hook.lastName != "success-job" {
			t.Fatalf("WithTracing(): lastName=%q, want=%q", hook.lastName, "success-job")
		}
		if !ctxSeen {
			t.Fatal("WithTracing(): expected job to receive context from Start")
		}
		if hook.lastDuration < 0 {
			t.Fatalf("WithTracing(): duration=%v, want>=0", hook.lastDuration)
		}
	})

	t.Run("failure_path", func(t *testing.T) {
		hook := &testTraceHook{}
		root := errors.New("boom")

		job := WithTracing(func(ctx context.Context) error {
			return root
		}, "failure-job", hook)

		err := job(context.Background())
		if !errors.Is(err, root) {
			t.Fatalf("WithTracing(): got=%v, want=%v", err, root)
		}
		if hook.startCalls != 1 {
			t.Fatalf("WithTracing(): startCalls=%d, want=1", hook.startCalls)
		}
		if hook.successCalls != 0 {
			t.Fatalf("WithTracing(): successCalls=%d, want=0", hook.successCalls)
		}
		if hook.failureCalls != 1 {
			t.Fatalf("WithTracing(): failureCalls=%d, want=1", hook.failureCalls)
		}
		if !errors.Is(hook.lastErr, root) {
			t.Fatalf("WithTracing(): hook error=%v, want=%v", hook.lastErr, root)
		}
	})
}


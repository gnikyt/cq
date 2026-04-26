package cq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueHooks_EnqueueStartResult(t *testing.T) {
	var (
		enqueued atomic.Int32
		started  atomic.Int32
		success  atomic.Int32
		failed   atomic.Int32
	)
	done := make(chan struct{}, 2)

	q := NewQueue(1, 2, 10, WithHooks(Hooks{
		OnEnqueue: func(event JobEvent) {
			enqueued.Add(1)
			if event.ID == "" {
				t.Error("expected enqueue event to include job ID")
			}
		},
		OnStart: func(event JobEvent) {
			started.Add(1)
		},
		OnSuccess: func(event JobEvent) {
			success.Add(1)
			done <- struct{}{}
		},
		OnFailure: func(event JobEvent) {
			failed.Add(1)
			if event.Err == nil {
				t.Error("expected failure event error to be set")
			}
			done <- struct{}{}
		},
	}))
	q.Start()
	defer q.Stop(true)

	q.Enqueue(func(ctx context.Context) error { return nil })
	q.Enqueue(func(ctx context.Context) error { return errors.New("boom") })

	waitDeadline := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-waitDeadline:
			t.Fatal("timed out waiting for hook events")
		}
	}

	if got := enqueued.Load(); got != 2 {
		t.Fatalf("got enqueued=%d, want 2", got)
	}
	if got := started.Load(); got != 2 {
		t.Fatalf("got started=%d, want 2", got)
	}
	if got := success.Load(); got != 1 {
		t.Fatalf("got success=%d, want 1", got)
	}
	if got := failed.Load(); got != 1 {
		t.Fatalf("got failed=%d, want 1", got)
	}
}

func TestQueueHooks_RescheduleFromReleaseSelf(t *testing.T) {
	var reschedules atomic.Int32

	q := NewQueue(1, 1, 10, WithHooks(Hooks{
		OnReschedule: func(event JobEvent) {
			reschedules.Add(1)
			if event.RescheduleReason != RescheduleReasonReleaseSelf {
				t.Fatalf("got reason=%q, want %q", event.RescheduleReason, RescheduleReasonReleaseSelf)
			}
		},
	}))
	q.Start()
	defer q.Stop(true)

	var calls atomic.Int32
	job := WithReleaseSelf(func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			_ = RequestRelease(ctx, 10*time.Millisecond)
		}
		return nil
	}, q, 1)

	q.Enqueue(job)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if calls.Load() >= 2 && reschedules.Load() >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("got calls=%d reschedules=%d, want at least 2 and 1", calls.Load(), reschedules.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestQueueHooks_PanicInHookReportedAndJobContinues(t *testing.T) {
	var panicCalls atomic.Int32
	var ran atomic.Bool

	q := NewQueue(1, 1, 10,
		WithPanicHandler(func(any) {
			panicCalls.Add(1)
		}),
		WithHooks(Hooks{
			OnStart: func(event JobEvent) {
				panic("hook boom")
			},
		}),
	)
	q.Start()
	defer q.Stop(true)

	q.Enqueue(func(ctx context.Context) error {
		ran.Store(true)
		return nil
	})

	deadline := time.Now().Add(1 * time.Second)
	for {
		if ran.Load() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for job run")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !ran.Load() {
		t.Fatal("expected job to run despite hook panic")
	}
	if panicCalls.Load() == 0 {
		t.Fatal("expected hook panic to be reported through panic handler")
	}
}

func TestQueueHooks_MultipleWithHooksAppend(t *testing.T) {
	var (
		first  atomic.Int32
		second atomic.Int32
	)

	q := NewQueue(1, 1, 10,
		WithHooks(Hooks{
			OnSuccess: func(event JobEvent) { first.Add(1) },
		}),
		WithHooks(Hooks{
			OnSuccess: func(event JobEvent) { second.Add(1) },
		}),
	)
	q.Start()
	defer q.Stop(true)

	q.Enqueue(func(ctx context.Context) error { return nil })

	deadline := time.Now().Add(1 * time.Second)
	for {
		if first.Load() == 1 && second.Load() == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("got first=%d second=%d, want 1 each", first.Load(), second.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

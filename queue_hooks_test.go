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

	q := NewQueue(1, 2, 10, WithQueueName("jobs"), WithHooks(Hooks{
		OnEnqueue: func(_ context.Context, event JobEvent) {
			enqueued.Add(1)
			if event.ID == "" {
				t.Error("expected enqueue event to include job ID")
			}
			if event.QueueName != "jobs" {
				t.Errorf("enqueue event queue name: got %q, want %q", event.QueueName, "jobs")
			}
		},
		OnStart: func(_ context.Context, event JobEvent) {
			started.Add(1)
			if event.WaitDuration < 0 {
				t.Error("expected non-negative wait duration")
			}
		},
		OnSuccess: func(_ context.Context, event JobEvent) {
			success.Add(1)
			if event.ExecutionDuration <= 0 {
				t.Error("expected positive execution duration")
			}
			done <- struct{}{}
		},
		OnFailure: func(_ context.Context, event JobEvent) {
			failed.Add(1)
			if event.Err == nil {
				t.Error("expected failure event error to be set")
			}
			done <- struct{}{}
		},
	}))
	q.Start()
	defer q.Stop(true)

	mustSubmit(t, q, func(ctx context.Context) error { return nil })
	mustSubmit(t, q, func(ctx context.Context) error { return errors.New("boom") })

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

func TestQueueHooks_AttemptHooksForRetryPolicy(t *testing.T) {
	var (
		attemptStarts  atomic.Int32
		attemptSuccess atomic.Int32
		attemptFailure atomic.Int32
	)
	done := make(chan struct{}, 1)

	q := NewQueue(1, 1, 10, WithHooks(Hooks{
		OnAttemptStart: func(_ context.Context, event JobEvent) {
			attemptStarts.Add(1)
			if event.Attempt < 0 {
				t.Fatalf("attempt start: got attempt=%d, want >=0", event.Attempt)
			}
		},
		OnAttemptFailure: func(_ context.Context, event JobEvent) {
			attemptFailure.Add(1)
			if event.Err == nil {
				t.Fatal("attempt failure event missing error")
			}
		},
		OnAttemptSuccess: func(_ context.Context, event JobEvent) {
			attemptSuccess.Add(1)
		},
		OnSuccess: func(_ context.Context, _ JobEvent) {
			done <- struct{}{}
		},
	}))
	q.Start()
	defer q.Stop(true)

	var tries atomic.Int32
	job := WithRetryPolicy(func(ctx context.Context) error {
		if tries.Add(1) < 3 {
			return errors.New("retry me")
		}
		return nil
	}, RetryPolicy{MaxAttempts: 3})

	mustSubmit(t, q, job)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retry job completion")
	}

	if got := attemptStarts.Load(); got != 3 {
		t.Fatalf("attempt starts: got %d, want 3", got)
	}
	if got := attemptFailure.Load(); got != 2 {
		t.Fatalf("attempt failures: got %d, want 2", got)
	}
	if got := attemptSuccess.Load(); got != 1 {
		t.Fatalf("attempt success: got %d, want 1", got)
	}
}

func TestQueueHooks_DiscardHookAndStats(t *testing.T) {
	discards := make(chan JobEvent, 1)
	var failures atomic.Int32
	q := NewQueue(1, 1, 1, WithHooks(Hooks{
		OnDiscard: func(_ context.Context, event JobEvent) {
			discards <- event
		},
		OnFailure: func(_ context.Context, event JobEvent) {
			failures.Add(1)
		},
	}))
	q.Start()
	defer q.Stop(true)

	mustSubmit(t, q, WithOutcome(func(context.Context) error {
		return AsDiscard(errors.New("drop"))
	}, nil, nil, nil))

	select {
	case event := <-discards:
		if event.State != JobStateDiscarded {
			t.Fatalf("discard event state: got %v, want %v", event.State, JobStateDiscarded)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for discard hook")
	}

	stats := q.Stats()
	if stats.DiscardedJobs != 1 {
		t.Fatalf("stats discarded jobs: got %d, want 1", stats.DiscardedJobs)
	}
	if got := failures.Load(); got != 0 {
		t.Fatalf("failure hooks on discard: got %d, want 0", got)
	}
}

func TestQueueHooks_RescheduleFromReleaseSelf(t *testing.T) {
	var reschedules atomic.Int32

	q := NewQueue(1, 1, 10, WithHooks(Hooks{
		OnReschedule: func(_ context.Context, event JobEvent) {
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

	mustSubmit(t, q, job)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if calls.Load() >= 2 && reschedules.Load() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("got calls=%d reschedules=%d, want at least 2 and 1", calls.Load(), reschedules.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats := q.Stats()
	if stats.RescheduledJobs < 1 {
		t.Fatalf("stats rescheduled jobs: got %d, want at least 1", stats.RescheduledJobs)
	}
	if stats.ReleasedJobs < 1 {
		t.Fatalf("stats released jobs: got %d, want at least 1", stats.ReleasedJobs)
	}
}

func TestQueueHooks_CancelledJobUsesCancelledState(t *testing.T) {
	events := make(chan JobEvent, 1)
	q := NewQueue(1, 1, 1, WithHooks(Hooks{
		OnFailure: func(_ context.Context, event JobEvent) {
			events <- event
		},
	}))
	q.Start()
	defer q.Stop(true)

	started := make(chan struct{})
	handle := mustSubmit(t, q, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	<-started
	handle.Cancel()

	event := <-events
	if event.State != JobStateCancelled {
		t.Fatalf("event state: got %v, want %v", event.State, JobStateCancelled)
	}
	if !errors.Is(event.Err, ErrJobCancelled) {
		t.Fatalf("event error: got %v, want %v", event.Err, ErrJobCancelled)
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
			OnStart: func(_ context.Context, event JobEvent) {
				panic("hook boom")
			},
		}),
	)
	q.Start()
	defer q.Stop(true)

	mustSubmit(t, q, func(ctx context.Context) error {
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
			OnSuccess: func(_ context.Context, event JobEvent) { first.Add(1) },
		}),
		WithHooks(Hooks{
			OnSuccess: func(_ context.Context, event JobEvent) { second.Add(1) },
		}),
	)
	q.Start()
	defer q.Stop(true)

	mustSubmit(t, q, func(ctx context.Context) error { return nil })

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

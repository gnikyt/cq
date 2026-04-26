package cq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestReschedule_InvalidArgs(t *testing.T) {
	if ok := Reschedule(context.Background(), nil, func(context.Context) error { return nil }, time.Millisecond, "x"); ok {
		t.Fatal("got ok=true, want false for nil queue")
	}
	if ok := Reschedule(context.Background(), NewQueue(0, 0, 1), nil, time.Millisecond, "x"); ok {
		t.Fatal("got ok=true, want false for nil job")
	}
}

func TestReschedule_EmitsHookAndReenqueues(t *testing.T) {
	var (
		rescheduled atomic.Int32
		executed    atomic.Int32
	)
	var (
		gotReason atomic.Value
		negDelay  atomic.Bool
	)
	done := make(chan struct{}, 1)

	q := NewQueue(1, 1, 10, WithHooks(Hooks{
		OnReschedule: func(event JobEvent) {
			gotReason.Store(event.RescheduleReason)
			if event.Delay < 0 {
				negDelay.Store(true)
			}
			rescheduled.Add(1)
		},
	}))
	q.Start()
	defer q.Stop(true)

	next := func(ctx context.Context) error {
		executed.Add(1)
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	}

	first := func(ctx context.Context) error {
		if ok := Reschedule(ctx, q, next, 25*time.Millisecond, "custom_reason"); !ok {
			t.Fatal("expected Reschedule to succeed")
		}
		return nil
	}

	q.Enqueue(first)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for re-enqueued job")
	}

	if got := rescheduled.Load(); got != 1 {
		t.Fatalf("got rescheduled=%d, want 1", got)
	}
	if got := executed.Load(); got != 1 {
		t.Fatalf("got executed=%d, want 1", got)
	}
	if negDelay.Load() {
		t.Fatal("got negative delay in reschedule event")
	}
	if reason, _ := gotReason.Load().(string); reason != "custom_reason" {
		t.Fatalf("got reason=%q, want %q", reason, "custom_reason")
	}
}

package cq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestReschedule_InvalidArgs(t *testing.T) {
	if handle, err := Reschedule(context.Background(), nil, func(context.Context) error { return nil }, time.Millisecond, "x"); handle != nil || !errors.Is(err, ErrRescheduleQueueRequired) {
		t.Fatalf("got (%v, %v), want (nil, %v)", handle, err, ErrRescheduleQueueRequired)
	}
	if handle, err := Reschedule(context.Background(), NewQueue(0, 0, 1), nil, time.Millisecond, "x"); handle != nil || !errors.Is(err, ErrRescheduleJobRequired) {
		t.Fatalf("got (%v, %v), want (nil, %v)", handle, err, ErrRescheduleJobRequired)
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
		OnReschedule: func(_ context.Context, event JobEvent) {
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
		handle, err := Reschedule(ctx, q, next, 25*time.Millisecond, "custom_reason")
		if err != nil || handle == nil {
			t.Fatalf("Reschedule(): got (%v, %v), want (handle, nil)", handle, err)
		}
		return nil
	}

	mustSubmit(
		t,
		q,
		first,
		WithJobName("original"),
		WithJobAttribute("tenant", "acme"),
	)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for resubmitted job")
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

func TestReschedule_PreservesMetadataAndAddsLineage(t *testing.T) {
	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	result := make(chan JobMeta, 1)
	next := func(ctx context.Context) error {
		result <- MetaFromContext(ctx)
		return nil
	}
	first := func(ctx context.Context) error {
		_, err := Reschedule(ctx, q, next, 5*time.Millisecond, RescheduleReasonManualRetry)
		return err
	}

	original := mustSubmit(
		t,
		q,
		first,
		WithJobID("original-id"),
		WithJobName("invoice"),
		WithJobAttribute("tenant", "acme"),
	)
	if err := original.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(original): %v", err)
	}

	select {
	case meta := <-result:
		if meta.ID == "original-id" {
			t.Fatal("rescheduled submission reused original ID")
		}
		if meta.Name != "invoice" || meta.Attributes["tenant"] != "acme" {
			t.Fatalf("metadata not preserved: %+v", meta)
		}
		if meta.Attributes[RescheduleAttributeParentID] != "original-id" {
			t.Fatalf("parent ID: got %q", meta.Attributes[RescheduleAttributeParentID])
		}
		if meta.Attributes[RescheduleAttributeRootID] != "original-id" {
			t.Fatalf("root ID: got %q", meta.Attributes[RescheduleAttributeRootID])
		}
		if meta.Attributes[RescheduleAttributeReason] != RescheduleReasonManualRetry {
			t.Fatalf("reason: got %q", meta.Attributes[RescheduleAttributeReason])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rescheduled submission")
	}
}

func TestReschedule_PreservesRootLineage(t *testing.T) {
	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	result := make(chan JobMeta, 1)
	last := func(ctx context.Context) error {
		result <- MetaFromContext(ctx)
		return nil
	}
	second := func(ctx context.Context) error {
		_, err := Reschedule(ctx, q, last, time.Millisecond, RescheduleReasonManualRetry)
		return err
	}
	first := func(ctx context.Context) error {
		_, err := Reschedule(ctx, q, second, time.Millisecond, RescheduleReasonManualRetry)
		return err
	}

	mustSubmit(t, q, first, WithJobID("root-id"))
	select {
	case meta := <-result:
		if meta.Attributes[RescheduleAttributeRootID] != "root-id" {
			t.Fatalf("root ID: got %q", meta.Attributes[RescheduleAttributeRootID])
		}
		if meta.Attributes[RescheduleAttributeParentID] == "root-id" {
			t.Fatal("parent ID should identify the intermediate submission")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rescheduled chain")
	}
}

func TestReschedule_ReturnsSubmissionError(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	q.Stop(false)

	handle, err := Reschedule(context.Background(), q, func(context.Context) error { return nil }, time.Second, "x")
	if handle != nil || !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("Reschedule(): got (%v, %v), want (nil, %v)", handle, err, ErrQueueStopped)
	}
}

func TestReschedule_ReturnsCancellableHandle(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)

	rescheduled := make(chan *JobHandle, 1)
	ran := make(chan struct{}, 1)
	original := mustSubmit(t, q, func(ctx context.Context) error {
		handle, err := Reschedule(ctx, q, func(context.Context) error {
			ran <- struct{}{}
			return nil
		}, time.Hour, RescheduleReasonManualRetry)
		if err != nil {
			return err
		}
		rescheduled <- handle
		return nil
	})
	if err := original.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(original): %v", err)
	}

	handle := <-rescheduled
	if !handle.Cancel() {
		t.Fatal("Cancel(): got false, want true")
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrJobCancelled) {
		t.Fatalf("Wait(rescheduled): got %v, want %v", err, ErrJobCancelled)
	}
	select {
	case <-ran:
		t.Fatal("cancelled rescheduled job executed")
	default:
	}
}

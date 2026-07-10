package cq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestStopDrainHandsBackBufferedJobs(t *testing.T) {
	// No workers... everything stays buffered.
	queue := NewQueue(0, 0, 10)
	queue.Start()

	var ran atomic.Int32
	handles := make([]*JobHandle, 0, 3)
	for range 3 {
		handle, err := queue.Submit(context.Background(), func(ctx context.Context) error {
			ran.Add(1)
			return nil
		}, WithJobName("buffered"))
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
		handles = append(handles, handle)
	}

	drained, err := queue.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 3 {
		t.Fatalf("StopDrain(): got %d drained jobs, want 3", len(drained))
	}
	if ran.Load() != 0 {
		t.Fatal("drained jobs must not have run")
	}
	for _, dj := range drained {
		if dj.Job == nil {
			t.Error("drained job function must be non-nil")
		}
		if dj.Meta.ID == "" || dj.Meta.Name != "buffered" {
			t.Errorf("unexpected drained meta: %+v", dj.Meta)
		}
	}
	for _, handle := range handles {
		if err := handle.Wait(context.Background()); !errors.Is(err, ErrQueueDrained) {
			t.Errorf("Wait(): got %v, want ErrQueueDrained", err)
		}
	}
	if got := queue.TallyOf(JobStatePending); got != 0 {
		t.Errorf("TallyOf(JobStatePending): got %d, want 0 after drain", got)
	}
	if got := queue.TallyOf(JobStateCreated); got != 0 {
		t.Errorf("TallyOf(JobStateCreated): got %d, want 0 after rollback", got)
	}

	// Handed-back jobs are resubmittable to another queue.
	second := NewQueue(1, 2, 10)
	second.Start()
	for _, dj := range drained {
		if _, err := second.Submit(context.Background(), dj.Job, WithJobID(dj.Meta.ID)); err != nil {
			t.Fatalf("resubmit: %v", err)
		}
	}
	second.Stop(true)
	if ran.Load() != 3 {
		t.Errorf("got runs=%d, want 3 after resubmit", ran.Load())
	}
}

func TestStopDrainWaitsForInflightJobs(t *testing.T) {
	queue := NewQueue(1, 1, 10)
	queue.Start()

	started := make(chan struct{})
	var completed atomic.Int32
	// Occupies the only worker.
	_, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		completed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	<-started

	// Buffered behind the busy worker.
	if _, err := queue.Submit(context.Background(), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("submit buffered: %v", err)
	}

	drained, err := queue.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if completed.Load() != 1 {
		t.Error("expected in-flight job to complete during drain")
	}
	if len(drained) != 1 {
		t.Errorf("StopDrain(): got %d drained jobs, want 1", len(drained))
	}
	if got := queue.TallyOf(JobStateCompleted); got != 1 {
		t.Errorf("TallyOf(JobStateCompleted): got %d, want 1", got)
	}
}

func TestStopDrainHandsBackDelayedSubmissions(t *testing.T) {
	queue := NewQueue(1, 2, 10)
	queue.Start()

	handle, err := queue.SubmitAfter(context.Background(), func(ctx context.Context) error {
		return nil
	}, time.Hour, WithJobName("delayed"))
	if err != nil {
		t.Fatalf("submit after: %v", err)
	}

	drained, err := queue.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 1 {
		t.Fatalf("StopDrain(): got %d drained jobs, want 1 delayed", len(drained))
	}
	if drained[0].Meta.Name != "delayed" {
		t.Errorf("unexpected drained meta: %+v", drained[0].Meta)
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrQueueDrained) {
		t.Errorf("Wait(): got %v, want ErrQueueDrained for delayed handle", err)
	}
}

func TestStopDrainContextTimeout(t *testing.T) {
	queue := NewQueue(1, 1, 10)
	queue.Start()

	started := make(chan struct{})
	_, err := queue.Submit(context.Background(), func(ctx context.Context) error {
		close(started)
		<-ctx.Done() // Runs until queue context is cancelled.
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := queue.StopDrain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopDrain(): got %v, want DeadlineExceeded", err)
	}
}

func TestStopDrainStoppedQueue(t *testing.T) {
	queue := NewQueue(1, 2, 10)
	queue.Start()
	queue.Stop(true)
	if _, err := queue.StopDrain(context.Background()); !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("StopDrain(): got %v, want ErrQueueStopped", err)
	}
}

func TestStopDrainSkipsCancelledBufferedJobs(t *testing.T) {
	queue := NewQueue(0, 0, 10)
	queue.Start()

	handle, err := queue.Submit(context.Background(), func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !handle.Cancel() {
		t.Fatal("expected cancel to succeed")
	}

	drained, err := queue.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 0 {
		t.Fatalf("StopDrain(): got %d drained jobs, want 0 for cancelled submission", len(drained))
	}
	if got := queue.TallyOf(JobStateCancelled); got != 1 {
		t.Errorf("TallyOf(JobStateCancelled): got %d, want 1", got)
	}
	if got := queue.TallyOf(JobStatePending); got != 0 {
		t.Errorf("TallyOf(JobStatePending): got %d, want 0", got)
	}
}

func TestStopDrainEmptyQueue(t *testing.T) {
	queue := NewQueue(1, 2, 10)
	queue.Start()
	drained, err := queue.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 0 {
		t.Fatalf("StopDrain(): got %d drained jobs, want 0", len(drained))
	}
	if !queue.IsStopped() {
		t.Fatal("expected queue to be stopped after drain")
	}
}

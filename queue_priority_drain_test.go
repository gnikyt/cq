package cq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// newStalledPriorityQueue builds a priority queue whose dispatcher effectively
// never fires, so submitted jobs stay in the priority buffers for inspection
// and drain tests. The base queue has no workers, reinforcing that nothing is
// forwarded or started.
func newStalledPriorityQueue(t *testing.T, baseOpts ...QueueOption) (*PriorityQueue, *Queue) {
	t.Helper()
	base := NewQueue(0, 0, 16, baseOpts...)
	base.Start()
	pq := NewPriorityQueue(base, 16, WithPriorityTick(time.Hour))
	return pq, base
}

func TestPriorityQueueStopDrainHandsBackBuffered(t *testing.T) {
	pq, base := newStalledPriorityQueue(t)
	defer base.Stop(false)

	var ran atomic.Int32
	for _, p := range []Priority{PriorityHighest, PriorityMedium, PriorityLowest} {
		mustPrioritySubmit(t, pq, func(ctx context.Context) error {
			ran.Add(1)
			return nil
		}, p, WithJobName("buffered"))
	}

	drained, err := pq.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("StopDrain(): %v", err)
	}
	if len(drained) != 3 {
		t.Fatalf("StopDrain(): got %d drained jobs, want 3", len(drained))
	}
	if ran.Load() != 0 {
		t.Fatal("drained jobs must not have run")
	}
	for _, dj := range drained {
		if dj.Job == nil || dj.Meta.Name != "buffered" {
			t.Errorf("StopDrain(): unexpected drained job %+v", dj.Meta)
		}
	}

	// Handed-back jobs are resubmittable and runnable elsewhere.
	second := NewQueue(1, 2, 16)
	second.Start()
	for _, dj := range drained {
		if _, err := second.Submit(context.Background(), dj.Job); err != nil {
			t.Fatalf("resubmit: %v", err)
		}
	}
	second.Stop(true)
	if ran.Load() != 3 {
		t.Errorf("got runs=%d, want 3 after resubmit", ran.Load())
	}
}

func TestPriorityQueueStopDrainHandsBackDelayed(t *testing.T) {
	pq, base := newStalledPriorityQueue(t)
	defer base.Stop(false)

	handle, err := pq.SubmitAfter(context.Background(), func(ctx context.Context) error { return nil }, PriorityHigh, time.Hour, WithJobName("delayed"))
	if err != nil {
		t.Fatalf("SubmitAfter(): %v", err)
	}

	drained, err := pq.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("StopDrain(): %v", err)
	}
	if len(drained) != 1 {
		t.Fatalf("StopDrain(): got %d drained jobs, want 1 delayed", len(drained))
	}
	if drained[0].Meta.Name != "delayed" {
		t.Errorf("StopDrain(): unexpected drained meta %+v", drained[0].Meta)
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrQueueDrained) {
		t.Errorf("Wait(): got %v, want ErrQueueDrained", err)
	}
}

func TestPriorityQueueStopDrainResolvesHandles(t *testing.T) {
	pq, base := newStalledPriorityQueue(t)
	defer base.Stop(false)

	handle := mustPrioritySubmit(t, pq, func(ctx context.Context) error { return nil }, PriorityMedium)
	if _, err := pq.StopDrain(context.Background()); err != nil {
		t.Fatalf("StopDrain(): %v", err)
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrQueueDrained) {
		t.Errorf("Wait(): got %v, want ErrQueueDrained", err)
	}
}

func TestPriorityQueueStopDrainAlreadyStopped(t *testing.T) {
	pq, base := newStalledPriorityQueue(t)
	defer base.Stop(false)

	pq.Stop(false)
	if _, err := pq.StopDrain(context.Background()); !errors.Is(err, ErrPriorityQueueStopped) {
		t.Fatalf("StopDrain(): got %v, want ErrPriorityQueueStopped", err)
	}
}

func TestPriorityQueueStopDrainStopsBaseQueue(t *testing.T) {
	pq, base := newStalledPriorityQueue(t)

	if _, err := pq.StopDrain(context.Background()); err != nil {
		t.Fatalf("StopDrain(): %v", err)
	}
	if !base.IsStopped() {
		t.Error("StopDrain() should stop the base queue")
	}
}

// TestPriorityQueueStopDrainEmitsAbandonHook verifies parity with base
// Queue.StopDrain: every handed-back job (buffered and delayed) emits an
// OnAbandon event through the base queue's hooks.
func TestPriorityQueueStopDrainEmitsAbandonHook(t *testing.T) {
	rec := &abandonRecorder{}
	pq, base := newStalledPriorityQueue(t, WithHooks(rec.hooks()))
	defer base.Stop(false)

	mustPrioritySubmit(t, pq, func(ctx context.Context) error { return nil }, PriorityHigh, WithJobName("buffered"))
	if _, err := pq.SubmitAfter(context.Background(), func(ctx context.Context) error { return nil }, PriorityLow, time.Hour, WithJobName("delayed")); err != nil {
		t.Fatalf("SubmitAfter(): %v", err)
	}

	if _, err := pq.StopDrain(context.Background()); err != nil {
		t.Fatalf("StopDrain(): %v", err)
	}

	events := rec.snapshot()
	if len(events) != 2 {
		t.Fatalf("OnAbandon: got %d events, want 2", len(events))
	}
	names := map[string]bool{}
	for _, event := range events {
		if event.State != JobStateAbandoned {
			t.Errorf("OnAbandon: got state %v, want JobStateAbandoned", event.State)
		}
		if !errors.Is(event.Err, ErrQueueDrained) {
			t.Errorf("OnAbandon: got err %v, want ErrQueueDrained", event.Err)
		}
		names[event.Name] = true
	}
	if !names["buffered"] || !names["delayed"] {
		t.Errorf("OnAbandon: got names %v, want buffered and delayed", names)
	}
}

package cq

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestPriorityQueueManager_RegisterAndNames(t *testing.T) {
	mgr := NewPriorityQueueManager()

	if err := mgr.Register("", newPriorityManagerTestQueue(t)); !errors.Is(err, ErrPriorityQueueManagerInvalidName) {
		t.Fatalf("Register(empty): got %v, want %v", err, ErrPriorityQueueManagerInvalidName)
	}
	if err := mgr.Register("high", nil); !errors.Is(err, ErrPriorityQueueManagerNilQueue) {
		t.Fatalf("Register(nil): got %v, want %v", err, ErrPriorityQueueManagerNilQueue)
	}

	if err := mgr.Register("low", newPriorityManagerTestQueue(t)); err != nil {
		t.Fatalf("Register(low): unexpected err: %v", err)
	}
	if err := mgr.Register("high", newPriorityManagerTestQueue(t)); err != nil {
		t.Fatalf("Register(high): unexpected err: %v", err)
	}
	if err := mgr.Register("high", newPriorityManagerTestQueue(t)); !errors.Is(err, ErrPriorityQueueManagerExists) {
		t.Fatalf("Register(duplicate): got %v, want %v", err, ErrPriorityQueueManagerExists)
	}

	names := mgr.Names()
	if !reflect.DeepEqual(names, []string{"high", "low"}) {
		t.Fatalf("Names(): got %v, want [high low]", names)
	}
}

func TestPriorityQueueManager_EnqueueAndTryEnqueue(t *testing.T) {
	mgr := NewPriorityQueueManager()
	highQ := newPriorityManagerTestQueue(t)
	lowQ := newPriorityManagerTestQueue(t)

	if err := mgr.Register("high", highQ); err != nil {
		t.Fatalf("Register(high): %v", err)
	}
	if err := mgr.Register("low", lowQ); err != nil {
		t.Fatalf("Register(low): %v", err)
	}

	defer mgr.StopAll(true)

	doneHigh := make(chan struct{}, 1)
	doneLow := make(chan struct{}, 1)
	if err := mgr.Enqueue("high", func(ctx context.Context) error {
		doneHigh <- struct{}{}
		return nil
	}, PriorityHighest); err != nil {
		t.Fatalf("Enqueue(high): unexpected err: %v", err)
	}
	if err := mgr.Enqueue("low", func(ctx context.Context) error {
		doneLow <- struct{}{}
		return nil
	}, PriorityLow); err != nil {
		t.Fatalf("Enqueue(low): unexpected err: %v", err)
	}

	select {
	case <-doneHigh:
	case <-time.After(1 * time.Second):
		t.Fatal("high priority queue job did not execute")
	}
	select {
	case <-doneLow:
	case <-time.After(1 * time.Second):
		t.Fatal("low priority queue job did not execute")
	}

	ok, err := mgr.TryEnqueue("high", func(ctx context.Context) error { return nil }, PriorityHigh)
	if err != nil || !ok {
		t.Fatalf("TryEnqueue(high): got (%v, %v), want (true, nil)", ok, err)
	}
}

func TestPriorityQueueManager_DelayEnqueue(t *testing.T) {
	mgr := NewPriorityQueueManager()
	pq := newPriorityManagerTestQueue(t)

	if err := mgr.Register("delayed", pq); err != nil {
		t.Fatalf("Register(delayed): %v", err)
	}

	defer mgr.StopAll(true)

	done := make(chan struct{}, 1)
	if err := mgr.DelayEnqueue("delayed", func(ctx context.Context) error {
		done <- struct{}{}
		return nil
	}, PriorityHigh, 100*time.Millisecond); err != nil {
		t.Fatalf("DelayEnqueue(delayed): unexpected err: %v", err)
	}

	select {
	case <-done:
		t.Fatal("delayed priority queue job executed too early")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("delayed priority queue job did not execute")
	}
}

func TestPriorityQueueManager_StopAndNotFound(t *testing.T) {
	mgr := NewPriorityQueueManager()
	pq := newPriorityManagerTestQueue(t)

	if err := mgr.Register("high", pq); err != nil {
		t.Fatalf("Register(high): %v", err)
	}

	if err := mgr.Stop("high", true); err != nil {
		t.Fatalf("Stop(high): unexpected err: %v", err)
	}

	if !pq.queue.IsStopped() {
		t.Fatal("Stop(high, true): underlying queue should be stopped")
	}

	if err := mgr.Stop("missing", true); !errors.Is(err, ErrPriorityQueueManagerNotFound) {
		t.Fatalf("Stop(missing): got %v, want %v", err, ErrPriorityQueueManagerNotFound)
	}
	if err := mgr.Enqueue("missing", func(ctx context.Context) error { return nil }, PriorityHigh); !errors.Is(err, ErrPriorityQueueManagerNotFound) {
		t.Fatalf("Enqueue(missing): got %v, want %v", err, ErrPriorityQueueManagerNotFound)
	}
	if ok, err := mgr.TryEnqueue("missing", func(ctx context.Context) error { return nil }, PriorityHigh); ok || !errors.Is(err, ErrPriorityQueueManagerNotFound) {
		t.Fatalf("TryEnqueue(missing): got (%v, %v), want (false, %v)", ok, err, ErrPriorityQueueManagerNotFound)
	}
	if err := mgr.DelayEnqueue("missing", func(ctx context.Context) error { return nil }, PriorityHigh, time.Millisecond); !errors.Is(err, ErrPriorityQueueManagerNotFound) {
		t.Fatalf("DelayEnqueue(missing): got %v, want %v", err, ErrPriorityQueueManagerNotFound)
	}
}

func newPriorityManagerTestQueue(t *testing.T) *PriorityQueue {
	t.Helper()

	queue := NewQueue(1, 1, 10)
	queue.Start()
	return NewPriorityQueue(queue, 10, WithPriorityTick(time.Millisecond))
}

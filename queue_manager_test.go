package cq

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestQueueManager_RegisterAndNames(t *testing.T) {
	mgr := NewQueueManager()

	if err := mgr.Register("", NewQueue(1, 1, 10)); !errors.Is(err, ErrQueueManagerInvalidName) {
		t.Fatalf("Register(empty): got %v, want %v", err, ErrQueueManagerInvalidName)
	}
	if err := mgr.Register("high", nil); !errors.Is(err, ErrQueueManagerNilQueue) {
		t.Fatalf("Register(nil): got %v, want %v", err, ErrQueueManagerNilQueue)
	}

	if err := mgr.Register("low", NewQueue(1, 1, 10)); err != nil {
		t.Fatalf("Register(low): unexpected err: %v", err)
	}
	if err := mgr.Register("high", NewQueue(1, 1, 10)); err != nil {
		t.Fatalf("Register(high): unexpected err: %v", err)
	}
	if err := mgr.Register("high", NewQueue(1, 1, 10)); !errors.Is(err, ErrQueueManagerExists) {
		t.Fatalf("Register(duplicate): got %v, want %v", err, ErrQueueManagerExists)
	}

	names := mgr.Names()
	if !reflect.DeepEqual(names, []string{"high", "low"}) {
		t.Fatalf("Names(): got %v, want [high low]", names)
	}
}

func TestQueueManager_EnqueueAndTryEnqueue(t *testing.T) {
	mgr := NewQueueManager()
	highQ := NewQueue(1, 1, 10)
	lowQ := NewQueue(1, 1, 10)

	if err := mgr.Register("high", highQ); err != nil {
		t.Fatalf("Register(high): %v", err)
	}
	if err := mgr.Register("low", lowQ); err != nil {
		t.Fatalf("Register(low): %v", err)
	}

	mgr.StartAll()
	defer mgr.StopAll(true)

	doneHigh := make(chan struct{}, 1)
	doneLow := make(chan struct{}, 1)
	if err := mgr.Enqueue("high", func(ctx context.Context) error {
		doneHigh <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Enqueue(high): unexpected err: %v", err)
	}
	if err := mgr.Enqueue("low", func(ctx context.Context) error {
		doneLow <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Enqueue(low): unexpected err: %v", err)
	}

	select {
	case <-doneHigh:
	case <-time.After(1 * time.Second):
		t.Fatal("high queue job did not execute")
	}
	select {
	case <-doneLow:
	case <-time.After(1 * time.Second):
		t.Fatal("low queue job did not execute")
	}

	ok, err := mgr.TryEnqueue("high", func(ctx context.Context) error { return nil })
	if err != nil || !ok {
		t.Fatalf("TryEnqueue(high): got (%v, %v), want (true, nil)", ok, err)
	}
}

func TestQueueManager_DelayEnqueue(t *testing.T) {
	mgr := NewQueueManager()
	q := NewQueue(1, 1, 10)

	if err := mgr.Register("delayed", q); err != nil {
		t.Fatalf("Register(delayed): %v", err)
	}

	q.Start()
	defer q.Stop(true)

	done := make(chan struct{}, 1)
	if err := mgr.DelayEnqueue("delayed", func(ctx context.Context) error {
		done <- struct{}{}
		return nil
	}, 100*time.Millisecond); err != nil {
		t.Fatalf("DelayEnqueue(delayed): unexpected err: %v", err)
	}

	select {
	case <-done:
		t.Fatal("delayed queue job executed too early")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("delayed queue job did not execute")
	}
}

func TestQueueManager_NotFound(t *testing.T) {
	mgr := NewQueueManager()

	if err := mgr.Start("missing"); !errors.Is(err, ErrQueueManagerNotFound) {
		t.Fatalf("Start(missing): got %v, want %v", err, ErrQueueManagerNotFound)
	}
	if err := mgr.Stop("missing", true); !errors.Is(err, ErrQueueManagerNotFound) {
		t.Fatalf("Stop(missing): got %v, want %v", err, ErrQueueManagerNotFound)
	}
	if err := mgr.Enqueue("missing", func(ctx context.Context) error { return nil }); !errors.Is(err, ErrQueueManagerNotFound) {
		t.Fatalf("Enqueue(missing): got %v, want %v", err, ErrQueueManagerNotFound)
	}
	if ok, err := mgr.TryEnqueue("missing", func(ctx context.Context) error { return nil }); ok || !errors.Is(err, ErrQueueManagerNotFound) {
		t.Fatalf("TryEnqueue(missing): got (%v, %v), want (false, %v)", ok, err, ErrQueueManagerNotFound)
	}
	if err := mgr.DelayEnqueue("missing", func(ctx context.Context) error { return nil }, time.Millisecond); !errors.Is(err, ErrQueueManagerNotFound) {
		t.Fatalf("DelayEnqueue(missing): got %v, want %v", err, ErrQueueManagerNotFound)
	}
}

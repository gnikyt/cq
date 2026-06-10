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

func TestQueueManager_SubmitAndNonBlockingSubmit(t *testing.T) {
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
	if _, err := mgr.Submit(context.Background(), "high", func(ctx context.Context) error {
		doneHigh <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit(high): unexpected err: %v", err)
	}
	if _, err := mgr.Submit(context.Background(), "low", func(ctx context.Context) error {
		doneLow <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Submit(low): unexpected err: %v", err)
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

	handle, err := mgr.Submit(context.Background(), "high", func(ctx context.Context) error { return nil }, WithNonBlocking())
	if err != nil || handle == nil {
		t.Fatalf("Submit(high): got (%v, %v), want (handle, nil)", handle, err)
	}
}

func TestQueueManager_Submit(t *testing.T) {
	mgr := NewQueueManager()
	q := NewQueue(1, 1, 10)
	if err := mgr.Register("work", q); err != nil {
		t.Fatalf("Register(work): %v", err)
	}
	q.Start()
	defer q.Stop(true)

	handle, err := mgr.Submit(context.Background(), "work", func(ctx context.Context) error {
		if got := MetaFromContext(ctx).Name; got != "managed" {
			t.Fatalf("job name: got %q, want %q", got, "managed")
		}
		return nil
	}, WithJobName("managed"))
	if err != nil {
		t.Fatalf("Submit(work): got err=%v, want nil", err)
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(): got err=%v, want nil", err)
	}
}

func TestQueueManager_SubmitAfter(t *testing.T) {
	mgr := NewQueueManager()
	q := NewQueue(1, 1, 10)

	if err := mgr.Register("delayed", q); err != nil {
		t.Fatalf("Register(delayed): %v", err)
	}

	q.Start()
	defer q.Stop(true)

	done := make(chan struct{}, 1)
	handle, err := mgr.SubmitAfter(context.Background(), "delayed", func(ctx context.Context) error {
		done <- struct{}{}
		return nil
	}, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("SubmitAfter(delayed): unexpected err: %v", err)
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
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("SubmitAfter().Wait(): unexpected err: %v", err)
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
	if handle, err := mgr.SubmitAfter(context.Background(), "missing", func(ctx context.Context) error { return nil }, time.Millisecond); handle != nil || !errors.Is(err, ErrQueueManagerNotFound) {
		t.Fatalf("SubmitAfter(missing): got (%v, %v), want (nil, %v)", handle, err, ErrQueueManagerNotFound)
	}
	if handle, err := mgr.Submit(context.Background(), "missing", func(context.Context) error { return nil }); handle != nil || !errors.Is(err, ErrQueueManagerNotFound) {
		t.Fatalf("Submit(missing): got (%v, %v), want (nil, %v)", handle, err, ErrQueueManagerNotFound)
	}
}

package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewQueueJobDispatcher(t *testing.T) {
	t.Run("dispatches_to_queue", func(t *testing.T) {
		q := NewQueue(1, 1, 10)
		q.Start()
		defer q.Stop(true)

		dispatched := make(chan struct{}, 1)
		dispatcher := NewQueueJobDispatcher(q)
		err := dispatcher.Dispatch(context.Background(), DispatchRequest{
			Key:    "k",
			Reason: DispatchReasonContention,
			Job: func(ctx context.Context) error {
				dispatched <- struct{}{}
				return nil
			},
		})
		if err != nil {
			t.Fatalf("Dispatch(): got %v, want nil", err)
		}

		select {
		case <-dispatched:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Dispatch(): timed out waiting for dispatched job execution")
		}
	})

	t.Run("nil_queue_is_unavailable", func(t *testing.T) {
		dispatcher := NewQueueJobDispatcher(nil)
		err := dispatcher.Dispatch(context.Background(), DispatchRequest{
			Key: "k",
			Job: func(ctx context.Context) error { return nil },
		})

		var derr *DispatchError
		if !errors.As(err, &derr) {
			t.Fatalf("Dispatch(): got %T, want *DispatchError", err)
		}
		if derr.Kind != DispatchErrorKindUnavailable {
			t.Fatalf("Dispatch(): kind got %q, want %q", derr.Kind, DispatchErrorKindUnavailable)
		}
		if !errors.Is(err, ErrJobDispatcherNilQueue) {
			t.Fatalf("Dispatch(): got %v, want wrapped %v", err, ErrJobDispatcherNilQueue)
		}
	})

	t.Run("stopped_queue_is_rejected", func(t *testing.T) {
		q := NewQueue(1, 1, 1)
		q.Start()
		q.Stop(false)

		dispatcher := NewQueueJobDispatcher(q)
		err := dispatcher.Dispatch(context.Background(), DispatchRequest{
			Key: "k",
			Job: func(ctx context.Context) error { return nil },
		})

		var derr *DispatchError
		if !errors.As(err, &derr) {
			t.Fatalf("Dispatch(): got %T, want *DispatchError", err)
		}
		if derr.Kind != DispatchErrorKindRejected {
			t.Fatalf("Dispatch(): kind got %q, want %q", derr.Kind, DispatchErrorKindRejected)
		}
		if !errors.Is(err, ErrQueueStopped) {
			t.Fatalf("Dispatch(): got %v, want wrapped %v", err, ErrQueueStopped)
		}
	})
}

func TestNewQueueManagerJobDispatcher(t *testing.T) {
	t.Run("routes_to_named_queue", func(t *testing.T) {
		mgr := NewQueueManager()
		q := NewQueue(1, 1, 10)
		q.Start()
		defer q.Stop(true)

		if err := mgr.Register("routed", q); err != nil {
			t.Fatalf("Register(): got %v, want nil", err)
		}

		dispatched := make(chan struct{}, 1)
		dispatcher := NewQueueManagerJobDispatcher(mgr, func(req DispatchRequest) (string, error) {
			return "routed", nil
		})

		err := dispatcher.Dispatch(context.Background(), DispatchRequest{
			Key: "customer:123",
			Job: func(ctx context.Context) error {
				dispatched <- struct{}{}
				return nil
			},
		})
		if err != nil {
			t.Fatalf("Dispatch(): got %v, want nil", err)
		}

		select {
		case <-dispatched:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Dispatch(): timed out waiting for dispatched job execution")
		}
	})

	t.Run("nil_route_is_unavailable", func(t *testing.T) {
		dispatcher := NewQueueManagerJobDispatcher(NewQueueManager(), nil)
		err := dispatcher.Dispatch(context.Background(), DispatchRequest{
			Key: "k",
			Job: func(ctx context.Context) error { return nil },
		})

		var derr *DispatchError
		if !errors.As(err, &derr) {
			t.Fatalf("Dispatch(): got %T, want *DispatchError", err)
		}
		if derr.Kind != DispatchErrorKindUnavailable {
			t.Fatalf("Dispatch(): kind got %q, want %q", derr.Kind, DispatchErrorKindUnavailable)
		}
		if !errors.Is(err, ErrJobDispatcherNilQueueRoute) {
			t.Fatalf("Dispatch(): got %v, want wrapped %v", err, ErrJobDispatcherNilQueueRoute)
		}
	})
}

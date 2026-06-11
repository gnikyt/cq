package cq

import (
	"context"
	"errors"
	"testing"
)

func TestWithErrorOnContention(t *testing.T) {
	t.Run("injects_contention_try", func(t *testing.T) {
		var saw bool
		job := WithErrorOnContention(func(ctx context.Context) error {
			saw = contentionTryFromContext(ctx)
			return nil
		})
		if err := job(context.Background()); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		if !saw {
			t.Fatal("WithErrorOnContention should inject contention-try context")
		}
	})

	t.Run("contention_error_passthrough", func(t *testing.T) {
		job := WithErrorOnContention(func(context.Context) error {
			return ErrUniqueContended
		})
		err := job(context.Background())
		if !errors.Is(err, ErrUniqueContended) {
			t.Fatalf("got %v, want %v", err, ErrUniqueContended)
		}
	})

	t.Run("non_contention_error_passthrough", func(t *testing.T) {
		wantErr := errors.New("boom")
		job := WithErrorOnContention(func(context.Context) error {
			return wantErr
		})
		err := job(context.Background())
		if !errors.Is(err, wantErr) {
			t.Fatalf("got %v, want %v", err, wantErr)
		}
	})

	t.Run("composes_with_release_predicate", func(t *testing.T) {
		err := WithErrorOnContention(func(context.Context) error {
			return ErrUniqueContended
		})(context.Background())
		if !IsContentionError(err) {
			t.Fatalf("IsContentionError(%v) = false, want true", err)
		}
	})
}

func TestContentionErrorAvailableFromJobHandle(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)

	handle, err := q.Submit(context.Background(), WithErrorOnContention(func(context.Context) error {
		return ErrUniqueContended
	}))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}

	<-handle.Done()
	result, ok := handle.Result()
	if !ok {
		t.Fatal("Result(): got incomplete result")
	}
	if !IsContentionError(result.Err) {
		t.Fatalf("IsContentionError(%v) = false, want true", result.Err)
	}
}

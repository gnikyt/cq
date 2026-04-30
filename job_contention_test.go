package cq

import (
	"context"
	"errors"
	"testing"
)

func TestJobDispatcherFunc(t *testing.T) {
	t.Run("dispatches_to_function", func(t *testing.T) {
		wantErr := errors.New("dispatch failed")
		var gotReq DispatchRequest
		var ran bool

		dispatcher := JobDispatcherFunc(func(ctx context.Context, req DispatchRequest) error {
			gotReq = req
			if err := req.Job(context.Background()); err != nil {
				t.Fatalf("job(): got %v, want nil", err)
			}
			return wantErr
		})

		err := dispatcher.Dispatch(context.Background(), DispatchRequest{
			Key: "customer:123",
			Job: func(ctx context.Context) error {
				ran = true
				return nil
			},
			Reason: DispatchReasonContention,
		})

		if !errors.Is(err, wantErr) {
			t.Fatalf("Dispatch(): got %v, want %v", err, wantErr)
		}
		if gotReq.Key != "customer:123" {
			t.Fatalf("Dispatch(): got key %q, want %q", gotReq.Key, "customer:123")
		}
		if gotReq.Reason != DispatchReasonContention {
			t.Fatalf("Dispatch(): got reason %v, want %v", gotReq.Reason, DispatchReasonContention)
		}
		if !ran {
			t.Fatal("Dispatch(): job did not run")
		}
	})

	t.Run("nil_function_returns_error", func(t *testing.T) {
		var dispatcher JobDispatcherFunc

		err := dispatcher.Dispatch(context.Background(), DispatchRequest{
			Key: "customer:123",
			Job: func(ctx context.Context) error {
				t.Fatal("Dispatch(): job should not run for nil dispatcher function")
				return nil
			},
		})

		var derr *DispatchError
		if !errors.As(err, &derr) {
			t.Fatalf("Dispatch(): got %T, want *DispatchError", err)
		}
		if derr.Kind != DispatchErrorKindUnavailable {
			t.Fatalf("Dispatch(): kind got %q, want %q", derr.Kind, DispatchErrorKindUnavailable)
		}
		if !errors.Is(err, ErrDispatcherNilFunc) {
			t.Fatalf("Dispatch(): got %v, want wrapped %v", err, ErrDispatcherNilFunc)
		}
	})
}

func TestWithDispatchOnContention(t *testing.T) {
	t.Run("dispatch_requires_dispatcher", func(t *testing.T) {
		job := WithDispatchOnContention(func(ctx context.Context) error {
			return ErrUniqueContended
		}, "test", nil)

		err := job(context.Background())
		if !errors.Is(err, ErrDispatchRequired) {
			t.Fatalf("got %v, want %v", err, ErrDispatchRequired)
		}
	})

	t.Run("non_contention_error_passthrough", func(t *testing.T) {
		wantErr := errors.New("boom")
		job := WithDispatchOnContention(func(ctx context.Context) error {
			return wantErr
		}, "test", nil)

		err := job(context.Background())
		if !errors.Is(err, wantErr) {
			t.Fatalf("got %v, want %v", err, wantErr)
		}
	})

	t.Run("contention_dispatches", func(t *testing.T) {
		wantErr := errors.New("dispatched")
		var got DispatchRequest
		d := JobDispatcherFunc(func(ctx context.Context, req DispatchRequest) error {
			got = req
			return wantErr
		})

		job := WithDispatchOnContention(func(ctx context.Context) error {
			return ErrUniqueContended
		}, "k", d)

		err := job(context.Background())
		if !errors.Is(err, wantErr) {
			t.Fatalf("got %v, want %v", err, wantErr)
		}
		if got.Reason != DispatchReasonContention || got.Key != "k" {
			t.Fatalf("request: %+v", got)
		}
		if got.Err != nil {
			t.Fatalf("contention dispatch: got Err %v, want nil", got.Err)
		}
	})

	t.Run("injects_contention_try_for_inner_job", func(t *testing.T) {
		d := JobDispatcherFunc(func(ctx context.Context, req DispatchRequest) error { return nil })
		job := WithDispatchOnContention(func(ctx context.Context) error {
			if !contentionTryFromContext(ctx) {
				t.Fatal("WithDispatchOnContention should inject contention-try context for the inner job")
			}
			return ErrUniqueContended
		}, "k", d)
		if err := job(context.Background()); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
	})
}

func TestWithDispatchOnError(t *testing.T) {
	t.Run("dispatch_requires_dispatcher", func(t *testing.T) {
		job := WithDispatchOnError(func(ctx context.Context) error {
			return errors.New("any")
		}, "test", nil)

		err := job(context.Background())
		if !errors.Is(err, ErrDispatchRequired) {
			t.Fatalf("got %v, want %v", err, ErrDispatchRequired)
		}
	})

	t.Run("any_error_dispatches", func(t *testing.T) {
		inner := errors.New("inner")
		var got DispatchRequest
		d := JobDispatcherFunc(func(ctx context.Context, req DispatchRequest) error {
			got = req
			return nil
		})
		job := WithDispatchOnError(func(ctx context.Context) error {
			return inner
		}, "key", d)

		if err := job(context.Background()); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		if got.Reason != DispatchReasonError {
			t.Fatalf("Reason got %v, want %v", got.Reason, DispatchReasonError)
		}
		if !errors.Is(got.Err, inner) {
			t.Fatalf("Err got %v, want %v", got.Err, inner)
		}
		if got.Key != "key" {
			t.Fatalf("Key got %q", got.Key)
		}
	})
}

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
		job := WithErrorOnContention(func(ctx context.Context) error {
			return ErrUniqueContended
		})
		err := job(context.Background())
		if !errors.Is(err, ErrUniqueContended) {
			t.Fatalf("got %v, want %v", err, ErrUniqueContended)
		}
	})

	t.Run("non_contention_error_passthrough", func(t *testing.T) {
		wantErr := errors.New("boom")
		job := WithErrorOnContention(func(ctx context.Context) error {
			return wantErr
		})
		err := job(context.Background())
		if !errors.Is(err, wantErr) {
			t.Fatalf("got %v, want %v", err, wantErr)
		}
	})

	t.Run("composes_with_release_predicate", func(t *testing.T) {
		// Verify the WithRelease integration: IsContentionError matches our wrapper output.
		err := WithErrorOnContention(func(ctx context.Context) error {
			return ErrUniqueContended
		})(context.Background())
		if !IsContentionError(err) {
			t.Fatalf("IsContentionError(%v) = false, want true", err)
		}
	})
}

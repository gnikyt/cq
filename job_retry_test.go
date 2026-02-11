package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithRetry(t *testing.T) {
	var calls int // Number of times job was called.
	retries := 2  // Number of retries to do.

	job := WithRetry(func(ctx context.Context) error {
		calls++
		return errors.New("error")
	}, retries)
	if err := job(context.Background()); err == nil {
		t.Error("WithRetry(): job should have errored")
	}
	if calls != retries {
		t.Errorf("WithRetry(): job ran %v times, want %v", calls, retries)
	}
}

func TestWithBackoff(t *testing.T) {
	retries := 2                             // Number of retries.
	tlimit := time.Duration(4 * time.Second) // One retry = 1 second, two = 2 seconds... (1s + 2s) + (1s buffer) = limit.

	ctx, ctxc := context.WithTimeout(context.TODO(), tlimit)
	defer ctxc()

	done := make(chan error)
	go func() {
		job := WithRetry(WithBackoff(func(ctx context.Context) error {
			return errors.New("error")
		}, nil), retries)
		done <- job(context.Background())
	}()
	select {
	case <-ctx.Done():
		t.Errorf("WithBackoff(): should have completed within %v for %v retries", tlimit, retries)
	case <-done:
		return
	}
}

func TestWithRetryIf(t *testing.T) {
	t.Run("nil_predicate_retries_on_any_error", func(t *testing.T) {
		var calls int
		limit := 3

		job := WithRetryIf(func(ctx context.Context) error {
			calls++
			return errors.New("retry me")
		}, limit, nil)

		if err := job(context.Background()); err == nil {
			t.Fatal("WithRetryIf(): expected error")
		}
		if calls != limit {
			t.Fatalf("WithRetryIf(): got %d calls, want %d", calls, limit)
		}
	})

	t.Run("predicate_false_stops_retries", func(t *testing.T) {
		var calls int
		limit := 5
		stopErr := errors.New("do not retry")

		job := WithRetryIf(func(ctx context.Context) error {
			calls++
			return stopErr
		}, limit, func(err error) bool {
			return false
		})

		err := job(context.Background())
		if !errors.Is(err, stopErr) {
			t.Fatalf("WithRetryIf(): got %v, want %v", err, stopErr)
		}
		if calls != 1 {
			t.Fatalf("WithRetryIf(): got %d calls, want 1", calls)
		}
	})

	t.Run("predicate_true_then_false", func(t *testing.T) {
		var calls int
		limit := 5
		retryable := errors.New("retryable")
		permanent := errors.New("permanent")

		job := WithRetryIf(func(ctx context.Context) error {
			calls++
			if calls < 3 {
				return retryable
			}
			return permanent
		}, limit, func(err error) bool {
			return errors.Is(err, retryable)
		})

		err := job(context.Background())
		if !errors.Is(err, permanent) {
			t.Fatalf("WithRetryIf(): got %v, want %v", err, permanent)
		}
		if calls != 3 {
			t.Fatalf("WithRetryIf(): got %d calls, want 3", calls)
		}
	})

	t.Run("ErrPermanent_stops_retries", func(t *testing.T) {
		var calls int
		job := WithRetryIf(func(ctx context.Context) error {
			calls++
			return AsPermanent(errors.New("permanent"))
		}, 5, nil) // predicate nil = retry any error, but sentinel overrides.
		err := job(context.Background())
		if !errors.Is(err, ErrPermanent) {
			t.Fatalf("got %v", err)
		}
		if calls != 1 {
			t.Fatalf("ErrPermanent should stop retries: got %d calls", calls)
		}
	})

	t.Run("ErrDiscard_stops_retries", func(t *testing.T) {
		var calls int
		job := WithRetryIf(func(ctx context.Context) error {
			calls++
			return AsDiscard(errors.New("discard"))
		}, 5, nil)
		err := job(context.Background())
		if !errors.Is(err, ErrDiscard) {
			t.Fatalf("got %v", err)
		}
		if calls != 1 {
			t.Fatalf("ErrDiscard should stop retries: got %d calls", calls)
		}
	})
}

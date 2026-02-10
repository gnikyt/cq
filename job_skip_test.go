package cq

import (
	"context"
	"errors"
	"testing"
)

func TestWithSkipIf(t *testing.T) {
	t.Run("skip_returns_nil", func(t *testing.T) {
		called := false
		job := WithSkipIf(
			func(ctx context.Context) error {
				called = true
				return errors.New("should not run")
			},
			func(ctx context.Context) bool { return true },
		)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithSkipIf(): got %v, want nil", err)
		}
		if called {
			t.Fatal("WithSkipIf(): expected job to be skipped")
		}
	})

	t.Run("not_skipped_runs_job", func(t *testing.T) {
		called := false
		root := errors.New("job error")
		job := WithSkipIf(
			func(ctx context.Context) error {
				called = true
				return root
			},
			func(ctx context.Context) bool { return false },
		)

		err := job(context.Background())
		if !errors.Is(err, root) {
			t.Fatalf("WithSkipIf(): got %v, want %v", err, root)
		}
		if !called {
			t.Fatal("WithSkipIf(): expected job to run")
		}
	})

	t.Run("nil_predicate_passthrough", func(t *testing.T) {
		called := false
		job := WithSkipIf(
			func(ctx context.Context) error {
				called = true
				return nil
			},
			nil,
		)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithSkipIf(): got %v, want nil", err)
		}
		if !called {
			t.Fatal("WithSkipIf(): job did not execute (passthrough)")
		}
	})
}

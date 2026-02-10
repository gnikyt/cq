package cq

import (
	"context"
	"errors"
	"testing"
)

func TestWithErrorClassifier(t *testing.T) {
	t.Run("classifies_retryable", func(t *testing.T) {
		root := errors.New("temporary failure")
		job := WithErrorClassifier(
			func(ctx context.Context) error { return root },
			func(err error) ErrorClass { return ErrorClassRetryable },
		)

		err := job(context.Background())
		if err == nil {
			t.Fatal("expected error")
		}
		if !IsClass(err, ErrorClassRetryable) {
			t.Fatalf("WithErrorClassifier(): got class %v, want retryable", err)
		}
		if !errors.Is(err, root) {
			t.Fatalf("WithErrorClassifier(): got %v, want wrapped root error", err)
		}
	})

	t.Run("classifies_permanent", func(t *testing.T) {
		root := errors.New("bad request")
		job := WithErrorClassifier(
			func(ctx context.Context) error { return root },
			func(err error) ErrorClass { return ErrorClassPermanent },
		)

		err := job(context.Background())
		if err == nil {
			t.Fatal("expected error")
		}
		if !IsClass(err, ErrorClassPermanent) {
			t.Fatalf("WithErrorClassifier(): got class %v, want permanent", err)
		}
	})

	t.Run("ignored_returns_nil", func(t *testing.T) {
		job := WithErrorClassifier(
			func(ctx context.Context) error { return errors.New("already done") },
			func(err error) ErrorClass { return ErrorClassIgnored },
		)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithErrorClassifier(): got %v, want nil (ignored class)", err)
		}
	})

	t.Run("nil_classifier_panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for nil classifier")
			}
		}()
		_ = WithErrorClassifier(
			func(ctx context.Context) error { return errors.New("unknown") },
			nil,
		)
	})

	t.Run("unknown_class_defaults_to_permanent", func(t *testing.T) {
		root := errors.New("weird")
		job := WithErrorClassifier(
			func(ctx context.Context) error { return root },
			func(err error) ErrorClass { return ErrorClass("unknown") },
		)

		err := job(context.Background())
		if err == nil {
			t.Fatal("expected error")
		}
		if !IsClass(err, ErrorClassPermanent) {
			t.Fatalf("WithErrorClassifier(): got class %v, want permanent (fallback)", err)
		}
	})

	t.Run("success_passthrough", func(t *testing.T) {
		called := false
		job := WithErrorClassifier(
			func(ctx context.Context) error { return nil },
			func(err error) ErrorClass {
				called = true
				return ErrorClassRetryable
			},
		)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithErrorClassifier(): got %v, want nil", err)
		}
		if called {
			t.Fatal("classifier should not be called on nil error")
		}
	})
}

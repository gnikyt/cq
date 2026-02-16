package cq

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestWithRecover(t *testing.T) {
	t.Run("panic_string", func(t *testing.T) {
		job := WithRecover(func(ctx context.Context) error {
			panic("string panic")
		})

		err := job(context.Background())
		if err == nil {
			t.Error("WithRecover(): should return error for panic")
		}
		if !strings.Contains(err.Error(), "job: panic: string panic") {
			t.Errorf("WithRecover(): got %v, want panic message", err)
		}
	})

	t.Run("panic_error", func(t *testing.T) {
		panicErr := errors.New("error panic")
		job := WithRecover(func(ctx context.Context) error {
			panic(panicErr)
		})

		err := job(context.Background())
		if err == nil {
			t.Error("WithRecover(): should return error for panic")
		}
		if !errors.Is(err, panicErr) {
			t.Errorf("WithRecover(): error chain broken, got %v", err)
		}
	})

	t.Run("panic_other", func(t *testing.T) {
		job := WithRecover(func(ctx context.Context) error {
			panic(12345)
		})

		err := job(context.Background())
		if err == nil {
			t.Error("WithRecover(): should return error for panic")
		}
		if !strings.Contains(err.Error(), "12345") {
			t.Errorf("WithRecover(): got %v, want panic value", err)
		}
	})

	t.Run("no_panic", func(t *testing.T) {
		job := WithRecover(func(ctx context.Context) error {
			return nil
		})

		err := job(context.Background())
		if err != nil {
			t.Errorf("WithRecover(): got error %v, want nil", err)
		}
	})

	t.Run("with_result_handler", func(t *testing.T) {
		var failedCalled bool
		var failedError error

		job := WithResultHandler(
			WithRecover(func(ctx context.Context) error {
				panic("panic in job")
			}),
			func() {
				t.Error("WithRecover(): onCompleted should not be called")
			},
			func(err error) {
				failedCalled = true
				failedError = err
			},
		)

		_ = job(context.Background())

		if !failedCalled {
			t.Error("WithRecover(): onFailed should have been called")
		}
		if failedError == nil {
			t.Error("WithRecover(): onFailed should receive error")
		}
		if !strings.Contains(failedError.Error(), "panic in job") {
			t.Errorf("WithRecover(): onFailed: got %v, want panic message", failedError)
		}
	})
}

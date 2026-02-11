package cq

import (
	"context"
	"errors"
	"testing"
)

func TestWithChain(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		job := func(ctx context.Context) error {
			return nil
		}
		job2 := func(ctx context.Context) error {
			return nil
		}
		chain := WithChain(job, job2)
		if err := chain(context.Background()); err != nil {
			t.Errorf("WithChain(): got %v, want nil", err)
		}
	})

	t.Run("failure", func(t *testing.T) {
		job := func(ctx context.Context) error {
			return nil
		}
		job2 := func(ctx context.Context) error {
			return errors.New("error")
		}
		job3 := func(ctx context.Context) error {
			t.Error("WithChain(): job3: should not have fired")
			return nil
		}
		chain := WithChain(job, job2, job3)
		if err := chain(context.Background()); err == nil {
			t.Errorf("WithChain(): got nil, want error")
		}
	})
}

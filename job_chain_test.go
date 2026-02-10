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

func TestWithPipeline(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		job := func(results chan int) Job {
			return func(ctx context.Context) error {
				results <- 1
				return nil
			}
		}
		job2 := func(results chan int) Job {
			return func(ctx context.Context) error {
				want := 1
				if val := <-results; val != want {
					t.Errorf("WithPipeline(): job2: got %v, want %v", val, want)
				}
				return nil
			}
		}
		pipeline := WithPipeline(job, job2)
		if err := pipeline(context.Background()); err != nil {
			t.Errorf("WithPipeline(): got %v, want nil", err)
		}
	})

	t.Run("error", func(t *testing.T) {
		job := func(results chan int) Job {
			return func(ctx context.Context) error {
				results <- 1
				return nil
			}
		}
		job2 := func(results chan int) Job {
			return func(ctx context.Context) error {
				want := 1
				if val := <-results; val != want {
					t.Errorf("WithPipeline(): job2: got result %v, want %v", val, want)
				}
				results <- 2
				return errors.New("error")
			}
		}
		job3 := func(results chan int) Job {
			return func(ctx context.Context) error {
				t.Error("WithPipeline(): job3: should not have fired")
				return nil
			}
		}
		pipeline := WithPipeline(job, job2, job3)
		if err := pipeline(context.Background()); err == nil {
			t.Errorf("WithPipeline(): got nil, want error")
		}
	})
}

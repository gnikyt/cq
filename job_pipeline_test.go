package cq

import (
	"context"
	"errors"
	"testing"
)

func TestWithPipeline(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		job1 := func(results chan int) Job {
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

		job3 := func(results chan string) Job {
			return func(ctx context.Context) error {
				results <- "test"
				return nil
			}
		}
		job4 := func(results chan string) Job {
			return func(ctx context.Context) error {
				want := "test"
				if val := <-results; val != want {
					t.Errorf("WithPipeline(): job4: got %v, want %v", val, want)
				}
				return nil
			}
		}

		pipeline := WithPipeline(job1, job2)
		if err := pipeline(context.Background()); err != nil {
			t.Errorf("WithPipeline(): got %v, want nil", err)
		}

		pipeline2 := WithPipeline(job3, job4)
		if err := pipeline2(context.Background()); err != nil {
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

package cq

import (
	"context"
	"errors"
	"testing"
)

func TestWithBatch(t *testing.T) {
	t.Run("all_success", func(t *testing.T) {
		var completed bool
		var completedErrors []error
		var progressCalls int

		jobs := []Job{
			func(ctx context.Context) error { return nil }, // Success.
			func(ctx context.Context) error { return nil }, // Success.
			func(ctx context.Context) error { return nil }, // Success.
		}

		batchJobs, state := WithBatch(
			jobs,
			func(errs []error) {
				completed = true
				completedErrors = errs
			},
			func(c, t int) { progressCalls++ },
		)

		// Run all jobs
		for _, job := range batchJobs {
			if err := job(context.Background()); err != nil {
				t.Errorf("WithBatch(): got %v, want nil", err)
			}
		}

		if !completed {
			t.Error("WithBatch(): onComplete should have been called")
		}
		if len(completedErrors) != 0 {
			t.Errorf("WithBatch(): should have no errors, got %d", len(completedErrors))
		}
		if progressCalls != 3 {
			t.Errorf("WithBatch(): progress calls: got %d, want 3", progressCalls)
		}
		if int(state.CompletedJobs.Load()) != 3 {
			t.Errorf("WithBatch(): completed jobs: got %d, want 3", state.CompletedJobs.Load())
		}
		if int(state.FailedJobs.Load()) != 0 {
			t.Errorf("WithBatch(): failed jobs: got %d, want 0", state.FailedJobs.Load())
		}
	})

	t.Run("with_failure", func(t *testing.T) {
		var completed bool
		var completedErrors []error

		jobs := []Job{
			func(ctx context.Context) error { return nil },                      // Success.
			func(ctx context.Context) error { return errors.New("job2 error") }, // Failure.
			func(ctx context.Context) error { return errors.New("job3 error") }, // Failure.
		}

		batchJobs, state := WithBatch(
			jobs,
			func(errs []error) {
				completed = true
				completedErrors = errs
			},
			nil,
		)

		// Run all jobs
		for _, job := range batchJobs {
			_ = job(context.Background())
		}

		if !completed {
			t.Error("WithBatch(): onComplete should have been called")
		}
		if len(completedErrors) != 2 {
			t.Errorf("WithBatch(): errors: got %d, want 2", len(completedErrors))
		}
		if int(state.CompletedJobs.Load()) != 3 {
			t.Errorf("WithBatch(): completed jobs: got %d, want 3", state.CompletedJobs.Load())
		}
		if int(state.FailedJobs.Load()) != 2 {
			t.Errorf("WithBatch(): failed jobs: got %d, want 2", state.FailedJobs.Load())
		}
	})

	t.Run("empty_jobs", func(t *testing.T) {
		jobs, state := WithBatch([]Job{}, nil, nil)
		if jobs != nil || state != nil {
			t.Error("WithBatch(): empty jobs should return nil")
		}
	})
}

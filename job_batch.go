package cq

import (
	"context"
	"sync"
	"sync/atomic"
)

// BatchState tracks runtime state for a batch of jobs.
type BatchState struct {
	TotalJobs     int32        // Total number of jobs in the batch.
	CompletedJobs atomic.Int32 // Number of jobs that have finished (success or failure).
	FailedJobs    atomic.Int32 // Number of jobs that have failed.

	OnComplete func([]error)              // Callback when all jobs finish. Receives all errors (empty if none).
	OnProgress func(completed, total int) // Optional callback executed after each job completes.

	Errors    []error    // Slice of all errors from failed jobs.
	errorsMut sync.Mutex // Mutex for protecting the Errors slice.
}

// WithBatch wraps jobs so they can be tracked as one logical batch.
// onComplete is called once when all jobs finish, with all errors (if any).
// onProgress is optional and called after each completed job.
func WithBatch(jobs []Job, onComplete func([]error), onProgress func(completed, total int)) ([]Job, *BatchState) {
	if len(jobs) == 0 {
		return nil, nil
	}

	state := &BatchState{
		TotalJobs:  int32(len(jobs)),
		Errors:     make([]error, 0),
		OnComplete: onComplete,
		OnProgress: onProgress,
	}

	wrappedJobs := make([]Job, len(jobs))
	for i, job := range jobs {
		wrappedJobs[i] = func(ojob Job) Job {
			return func(ctx context.Context) error {
				err := ojob(ctx)
				if err != nil {
					state.FailedJobs.Add(1)
					state.errorsMut.Lock()
					state.Errors = append(state.Errors, err)
					state.errorsMut.Unlock()
				}

				// Increment completed jobs.
				completed := state.CompletedJobs.Add(1)

				if state.OnProgress != nil {
					// Call onProgress callback, if provided.
					state.OnProgress(int(completed), int(state.TotalJobs))
				}

				// Check if this is the last job.
				if completed == state.TotalJobs && state.OnComplete != nil {
					// Call onComplete callback with all errors (empty if none).
					state.OnComplete(state.Errors)
				}

				return err
			}
		}(job)
	}

	return wrappedJobs, state
}

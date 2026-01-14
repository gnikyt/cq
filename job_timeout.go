package cq

import (
	"context"
	"time"
)

// WithTimeout accepts a timeout duration for which the job must
// timeout if not completed. A context is created and the job is
// ran in goroutine where it's error result is passed to a channel
// waiting for the result.
func WithTimeout(job Job, timeout time.Duration) Job {
	return func(ctx context.Context) error {
		// Create a new context with timeout, but inherit cancellation from parent
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- job(timeoutCtx)
		}()

		select {
		case <-ctx.Done():
			return ctx.Err() // Context cancelled, return error.
		case <-timeoutCtx.Done():
			return timeoutCtx.Err() // Timeout context cancelled, return error.
		case err := <-done:
			return err // Job completed (with error or nil), return error.
		}
	}
}

// WithDeadline accepts a time for which the job must completed by.
// A context is created and the job is ran in goroutine where it's
// error result is passed to a channel waiting for the result.
func WithDeadline(job Job, deadline time.Time) Job {
	return func(ctx context.Context) error {
		// Create a new context with deadline, but inherit cancellation from parent
		deadlineCtx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- job(deadlineCtx)
		}()

		select {
		case <-ctx.Done():
			return ctx.Err() // Context cancelled, return error.
		case <-deadlineCtx.Done():
			return deadlineCtx.Err() // Deadline context cancelled, return error.
		case err := <-done:
			return err // Job completed (with error or nil), return error.
		}
	}
}

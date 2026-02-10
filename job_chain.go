package cq

import (
	"context"
)

// WithChain composes jobs so each job runs only after the previous one succeeds.
// If any job returns an error, execution stops and remaining jobs are skipped.
// To pass values between jobs, use WithPipeline or your own channel.
func WithChain(jobs ...Job) Job {
	return func(ctx context.Context) error {
		for _, job := range jobs {
			if err := job(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

// WithPipeline is similar to WithChain, but creates a typed buffered channel.
// Each provided function receives that channel and returns a Job.
// Use this to pass values between chained jobs without wiring your own channel.
func WithPipeline[T any](jobs ...func(chan T) Job) Job {
	results := make(chan T, 1)
	return func(ctx context.Context) error {
		for _, job := range jobs {
			if err := job(results)(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

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

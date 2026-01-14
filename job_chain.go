package cq

import (
	"context"
)

// WithChain allows you to chain multiple jobs together where each
// job must complete before moving to the next. Should one of the
// jobs in the chain cause an error, the rest of the jobs will be
// discarded. If you would like to pass results from one job to the
// next, you can utilize a buffered channel or WithPipeline.
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

// WithPipeline is identical in function to WithChain except
// it will create a buffered channel of a supplied type to
// pass into each job so that you do not have to create your own
// channel setup. Each job must be wrapped to accept the channel
// as a parameter.
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

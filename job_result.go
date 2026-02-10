package cq

import (
	"context"
)

// WithResultHandler invokes callbacks on job success or failure.
// onCompleted runs after a nil error; onFailed runs with the returned error.
func WithResultHandler(job Job, onCompleted func(), onFailed func(error)) Job {
	return func(ctx context.Context) error {
		if err := job(ctx); err != nil {
			if onFailed != nil {
				onFailed(err)
			}
			return err
		}
		if onCompleted != nil {
			onCompleted()
		}
		return nil
	}
}

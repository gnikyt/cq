package cq

import (
	"context"
)

// WithResultHandler allows for notifying of the job completing or failing.
// If completed, the onCompleted function will execute.
// If failed, the onFailed function will execute and be passed in the error.
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

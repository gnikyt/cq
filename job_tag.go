package cq

import (
	"context"
)

// WithTagged wraps a job with tags for tracking and cancellation purposes.
// The job is registered with the provided tags and can be cancelled
// using the registry's CancelForTag method.
// The job automatically unregisters itself when it completes or fails.
func WithTagged(job Job, registry *JobRegistry, tags ...string) Job {
	return func(ctx context.Context) error {
		jobID := registry.NextID()
		jobCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		registry.Register(jobID, tags, cancel)
		defer registry.Unregister(jobID, tags)

		return job(jobCtx)
	}
}

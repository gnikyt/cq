package cq

import "context"

// SkipIfFunc decides whether a job should be skipped.
// Returning true skips execution and returns nil.
type SkipIfFunc func(context.Context) bool

// WithSkipIf skips job execution when shouldSkip returns true.
// Skipped jobs return nil and are treated as successfully handled.
func WithSkipIf(job Job, shouldSkip SkipIfFunc) Job {
	if shouldSkip == nil {
		return job
	}

	return func(ctx context.Context) error {
		if shouldSkip(ctx) {
			return nil
		}
		return job(ctx)
	}
}


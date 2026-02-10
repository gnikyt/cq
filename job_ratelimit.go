package cq

import (
	"context"

	"golang.org/x/time/rate"
)

// WithRateLimit wraps a job with rate limiting using a token bucket algorithm.
// The limiter controls how many jobs can execute per second.
// If the limiter cannot provide a token before the context deadline,
// the job returns the context error.
func WithRateLimit(job Job, limiter *rate.Limiter) Job {
	return func(ctx context.Context) error {
		if err := limiter.Wait(ctx); err != nil {
			return err
		}
		return job(ctx)
	}
}

package cq

import (
	"context"
	"sync/atomic"

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

// WithRateLimitRelease wraps a job with rate limiting using a token bucket
// algorithm, but releases (re-enqueues) jobs when they would otherwise wait.
//
// Behavior:
//   - If a token is immediately available, the job executes now.
//   - If not, the job is delayed by the limiter reservation delay and re-enqueued,
//     returning nil immediately so the worker can process other jobs.
//   - maxReleases == 0 means unlimited releases.
//   - If maxReleases is exhausted, it falls back to blocking Wait behavior.
func WithRateLimitRelease(job Job, limiter *rate.Limiter, queue *Queue, maxReleases int) Job {
	if maxReleases < 0 {
		maxReleases = 0
	}

	var releases atomic.Int32
	var wrappedJob Job

	wrappedJob = func(ctx context.Context) error {
		res := limiter.Reserve()
		if !res.OK() {
			// Fallback to blocking path if limiter cannot reserve.
			if err := limiter.Wait(ctx); err != nil {
				return err // Fallback to blocking path if limiter cannot reserve.
			}
			return job(ctx) // Job executed.
		}

		delay := res.Delay()
		if delay <= 0 {
			return job(ctx) // Job executed.
		}

		// We are not consuming now; return reservation to limiter.
		res.Cancel()

		if maxReleases == 0 {
			queue.DelayEnqueue(wrappedJob, delay)
			return nil // Unlimited releases, delay and re-enqueue.
		}

		for {
			current := releases.Load()
			if int(current) >= maxReleases {
				// Release budget exhausted... fallback to blocking path.
				if err := limiter.Wait(ctx); err != nil {
					return err // Fallback to blocking path if limiter cannot reserve.
				}
				return job(ctx) // Job executed.
			}
			if releases.CompareAndSwap(current, current+1) {
				queue.DelayEnqueue(wrappedJob, delay)
				return nil // Release budget allows, delay and re-enqueue.
			}
		}
	}

	return wrappedJob
}

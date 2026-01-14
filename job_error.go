package cq

import (
	"context"
	"fmt"
	"time"
)

// WithRelease re-enqueues a job after a delay if it returns an error
// that matches the shouldRelease predicate. The shouldRelease function
// should return true for errors that warrant a re-enqueue (such as network
// timeouts, rate limits, etc). If maxReleases is 0, the job will be released
// indefinitely. If maxReleases is exceeded or shouldRelease returns false,
// the error is returned and the job is marked as failed.
func WithRelease(job Job, queue *Queue, delay time.Duration, maxReleases int, shouldRelease func(error) bool) Job {
	var releases int
	var wrappedJob Job
	wrappedJob = func(ctx context.Context) error {
		err := job(ctx)
		if err != nil && shouldRelease(err) {
			if maxReleases == 0 || releases < maxReleases {
				releases++
				queue.DelayEnqueue(wrappedJob, delay)
				return nil
			}
		}
		return err
	}
	return wrappedJob
}

// WithRecover wraps a job to recover from panics and return them as errors.
// This allows panic errors to be handled by WithResultHandler and similar wrappers.
// Without this wrapper, panics are caught by the queue and logged to stderr (or sent
// to the panic handler if configured).
func WithRecover(job Job) Job {
	return func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				switch x := r.(type) {
				case string:
					err = fmt.Errorf("job panic: %s", x)
				case error:
					err = fmt.Errorf("job panic: %w", x)
				default:
					err = fmt.Errorf("job panic: %v", x)
				}
			}
		}()
		return job(ctx)
	}
}

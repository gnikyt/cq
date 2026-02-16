package cq

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// BackoffFunc returns the delay before the next retry attempt.
type BackoffFunc func(retries int) time.Duration

// WithRetry retries a job up to `limit` times until it succeeds.
// Retries happen immediately unless composed with WithBackoff.
func WithRetry(job Job, limit int) Job {
	return WithRetryIf(job, limit, nil)
}

// WithRetryIf retries a job up to `limit` times while shouldRetry(err) is true.
// shouldRetry is optional... if nil, any non-nil error is retried.
// Each attempt increments JobMeta.Attempt in the context.
func WithRetryIf(job Job, limit int, shouldRetry func(error) bool) Job {
	if shouldRetry == nil {
		shouldRetry = func(err error) bool { return err != nil }
	}

	return func(ctx context.Context) error {
		var err error
		for attempt := range limit {
			// Update JobMeta with current attempt number.
			meta := MetaFromContext(ctx)
			meta.Attempt = attempt
			attemptCtx := contextWithMeta(ctx, meta)

			if err = job(attemptCtx); err == nil {
				break // Success, no retry needed.
			}
			if isPermanent(err) || isDiscarded(err) {
				break // Permanent or discarded, no retry.
			}
			if !shouldRetry(err) {
				break // Not retryable due to predicate, no retry.
			}
		}
		return err // Retry limit reached or no retry needed.
	}
}

// WithBackoff adds a delay strategy between repeated executions.
// It is typically composed inside WithRetry: WithRetry(WithBackoff(job, bf), n).
// If bf is nil, ExponentialBackoff is used.
func WithBackoff(job Job, bf BackoffFunc) Job {
	if bf == nil {
		bf = ExponentialBackoff
	}
	return func(ctx context.Context) error {
		meta := MetaFromContext(ctx)
		if meta.Attempt > 0 {
			timer := time.NewTimer(bf(meta.Attempt))
			defer timer.Stop()

			select {
			case <-ctx.Done():
				return ctx.Err() // Context cancelled while waiting for backoff.
			case <-timer.C:
			}
		}
		return job(ctx)
	}
}

// ExponentialBackoff is the default backoff implementation.
// For each retry, it will exponentially increase the time.
// 1s,1s,2s,4s,8s...
// Based on https://www.instana.com/blog/exponential-back-off-algorithms/.
func ExponentialBackoff(retries int) time.Duration {
	return time.Duration(math.Ceil(.5*math.Pow(float64(2), float64(retries)))) * time.Second
}

// FibonacciBackoff uses a Fibonacci sequence based on retry count.
// 0s,1s,1s,2s,3s,5s,8s...
// Based on EventSaucePHP/BackOff.
func FibonacciBackoff(retries int) time.Duration {
	phi := 1.6180339887499 // (1 + sqrt(5)) / 2
	return time.Duration((math.Pow(phi, float64(retries))-math.Pow((1-phi), float64(retries)))/math.Sqrt(5)) * time.Second
}

// JitterBackoff returns randomized delays based on retry count.
// 717.00ms,903ms,10s,4s,53s...
func JitterBackoff(retries int) time.Duration {
	offset := .1 + rand.Float64()*(.8-.1) // Random float between 100ms and 800ms.
	max := float64(math.Floor(math.Pow(float64(2), float64(retries))*.5)) + float64(offset)
	return time.Duration((offset + rand.Float64()*(max-offset)) * float64(time.Second))
}

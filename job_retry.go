package cq

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// BackoffFunc takes the current number of retries and delays
// the next execution of the job based on a provided time duration.
type BackoffFunc func(retries int) time.Duration

// WithRetry allows for the job to be retried up to the limit.
// It will immediately keep calling the job until the limit
// is reached. Backoff support between retries can be added
// by using WithBackoff.
func WithRetry(job Job, limit int) Job {
	var retries int
	var err error
	return func(ctx context.Context) error {
		for retries < limit {
			if err = job(ctx); err != nil {
				retries++
			} else {
				break
			}
		}
		return err
	}
}

// WithBackoff is to be used with WithRetry to allow backoffs to
// happen in between reexecuting the job. A backoff function can be
// provided as the second parameter, or the default implementation
// of exponential will be used.
func WithBackoff(job Job, bf BackoffFunc) Job {
	var calls int
	if bf == nil {
		bf = ExponentialBackoff
	}
	return func(ctx context.Context) error {
		if calls > 0 {
			<-time.After(bf(calls))
		}
		calls++
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

// FibonacciBackoff is a backoff implementation which returns a number
// reprecenting the previous two numbers combined, based off the number
// of retries.
// 0s,1s,1s,2s,3s,5s,8s...
// Based on EventSaucePHP/BackOff.
func FibonacciBackoff(retries int) time.Duration {
	phi := 1.6180339887499 // (1 + sqrt(5)) / 2
	return time.Duration((math.Pow(phi, float64(retries))-math.Pow((1-phi), float64(retries)))/math.Sqrt(5)) * time.Second
}

// JitterBackoff is a backoff implementation which randomly
// produces a number based upon the number of retries and a random
// offset.
// 717.00ms,903ms,10s,4s,53s...
func JitterBackoff(retries int) time.Duration {
	offset := .1 + rand.Float64()*(.8-.1) // Random float between 100ms and 800ms.
	max := float64(math.Floor(math.Pow(float64(2), float64(retries))*.5)) + float64(offset)
	return time.Duration((offset + rand.Float64()*(max-offset)) * float64(time.Second))
}

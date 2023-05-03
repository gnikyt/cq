package cq

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// JobState is the state of the job, used for queue tally lookups.
type JobState int

const (
	JobStateCreated JobState = iota
	JobStatePending
	JobStateActive
	JobStateFailed
	JobStateCompleted
)

// String() support for JobState.
func (js JobState) String() string {
	return [5]string{"created", "pending", "active", "failed", "completed"}[js]
}

// Job is type alias for the job signature.
type Job = func() error

// WithResultHandler allows for notifying of the job completing or failing.
// If completed, the completed function will execute.
// If failed, the failed function will execute and be passed in the error.
func WithResultHandler(job Job, completed func(), failed func(error)) Job {
	return func() error {
		if err := job(); err != nil {
			if failed != nil {
				failed(err)
			}
			return err
		}
		if completed != nil {
			completed()
		}
		return nil
	}
}

// WithRetry allows for the job to be retried up to the limit.
// It will immediately keep calling the job until the limit
// is reached. Backoff support between retries can be added
// by using WithBackoff.
func WithRetry(job Job, limit int) Job {
	var retries int
	var err error
	return func() error {
		for retries < limit {
			if err = job(); err != nil {
				retries++
			} else {
				break
			}
		}
		return err
	}
}

// BackoffFunc takes the current number of retries and delays
// the next execution of the job based on a provided time duration.
type BackoffFunc func(retries int) time.Duration

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
	offset := .1 + rand.Float64()*(.8-.1) // Random float between 100ms and 800ms
	max := float64(math.Floor(math.Pow(float64(2), float64(retries))*.5)) + float64(offset)
	return time.Duration((offset + rand.Float64()*(max-offset)) * float64(time.Second))
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
	return func() error {
		if calls > 0 {
			<-time.After(bf(calls))
		}
		calls++
		return job()
	}
}

// WithTimeout accepts a timeout duration for which the job must
// timeout if not completed. A context is created and the job is
// ran in goroutine where it's error result is passed to a channel
// waiting for the result.
func WithTimeout(job Job, timeout time.Duration) Job {
	return func() error {
		ctx, ctxc := context.WithTimeout(context.Background(), timeout)
		defer ctxc()

		done := make(chan error, 1)
		go func() { done <- job() }()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		}
	}
}

// WithDeadline accepts a time for which the job must completed by.
// A context is created and the job is ran in goroutine where it's
// error result is passed to a channel waiting for the result.
func WithDeadline(job Job, deadline time.Time) Job {
	return func() error {
		ctx, ctxc := context.WithDeadline(context.Background(), deadline)
		defer ctxc()

		done := make(chan error, 1)
		go func() { done <- job() }()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		}
	}
}

// WithoutOverlap is a Laravel-inspired feature to ensure multiple jobs
// of a given key cannot run at the same time. This is useful in an example
// of where multiple jobs are touching the same source of data, such as
// a dollar amount, where the amount must must be decremented one at a time
// without race conditions.
func WithoutOverlap(job Job, key string, locker Locker[*sync.Mutex]) Job {
	return func() error {
		var mut *sync.Mutex
		lock, exists := locker.Get(key)
		if exists {
			mut = lock.Value
		} else {
			mut = &sync.Mutex{}
		}
		mut.Lock()
		defer mut.Unlock()
		if !exists {
			// Aquire a new lock for this job since one does not exist.
			locker.Aquire(key, LockValue[*sync.Mutex]{
				ExpiresAt: time.Time{},
				Value:     mut,
			})
		}
		return job()
	}
}

// WithUnqiue is a Laravel-inspired feature which will ensure a job
// of a given key can must be unique for a duration of time (ut).
// If the duration of time has passed, the job will be allowed to fire.
// If the duration of time (ut) is zero, default will be
// time.Now()+1s.
// This could be useful in an example of where you want to ensure a
// notification email can only be sent once every 5 minutes.
// Note: Ideally the job would be kicked out before being pushed into
// the queue,however that would require the jobs to be more than just
// functions, it would require them to be structs with state, which
// adds overhead to the setup.
func WithUnique(job Job, key string, ut time.Duration, locker Locker[struct{}]) Job {
	return func() error {
		lock, exists := locker.Get(key)
		if exists {
			if !lock.IsExpired() {
				// Return this job as "done" since original job has not yet completed.
				return nil
			} else {
				// Lock exists, but is expired, release it. In this event, the job may have
				// not been processed yet, took too long to complete, etc.
				locker.Release(key)
			}
		}
		// Lock either doesnt exist or was released, aquire a new lock.
		var es struct{}
		locker.Aquire(key, LockValue[struct{}]{
			ExpiresAt: time.Now().Add(ut),
			Value:     es,
		})
		defer locker.Release(key)
		return job()
	}
}

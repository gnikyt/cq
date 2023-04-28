package cq

import (
	"context"
	"math"
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

// BackoffFunc takes the current number of retries and delays
// the next execution of the job based on a provided time duration.
type BackoffFunc func(retries int) time.Duration

// defaultBackoffFunc is the default exponential backoff calculation
// Based on https://www.instana.com/blog/exponential-back-off-algorithms/.
func defaultBackoffFunc(retries int) time.Duration {
	return time.Duration(math.Ceil(.5*math.Pow(float64(2), float64(retries)))) * time.Second
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

// WithBackoff is to be used with WithRetry to allow backoffs to
// happen in between reexecuting the job. A backoff function can be
// provided as the second parameter, or the default implementation
// of exponential will be used.
func WithBackoff(job Job, bf BackoffFunc) Job {
	var calls int
	if bf == nil {
		bf = defaultBackoffFunc
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

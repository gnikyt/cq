package cq

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
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
type Job = func(ctx context.Context) error

// BatchState tracks the state of a batch of jobs.
type BatchState struct {
	TotalJobs     int32        // Total number of jobs in the batch.
	CompletedJobs atomic.Int32 // Number of jobs that have completed (both successes and failures).
	FailedJobs    atomic.Int32 // Number of jobs that have failed.

	OnComplete func()                     // Callback executed when all jobs complete successfully.
	OnFailure  func([]error)              // Callback executed when any job fails, passed all errors.
	OnProgress func(completed, total int) // Optional callback executed after each job completes.

	Errors    []error    // Slice of all errors from failed jobs.
	errorsMut sync.Mutex // Mutex for protecting the Errors slice.
}

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
	return func(ctx context.Context) error {
		if calls > 0 {
			<-time.After(bf(calls))
		}
		calls++
		return job(ctx)
	}
}

// WithTimeout accepts a timeout duration for which the job must
// timeout if not completed. A context is created and the job is
// ran in goroutine where it's error result is passed to a channel
// waiting for the result.
func WithTimeout(job Job, timeout time.Duration) Job {
	return func(ctx context.Context) error {
		// Create a new context with timeout, but inherit cancellation from parent
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- job(timeoutCtx)
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
		case err := <-done:
			return err
		}
	}
}

// WithDeadline accepts a time for which the job must completed by.
// A context is created and the job is ran in goroutine where it's
// error result is passed to a channel waiting for the result.
func WithDeadline(job Job, deadline time.Time) Job {
	return func(ctx context.Context) error {
		// Create a new context with deadline, but inherit cancellation from parent
		deadlineCtx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- job(deadlineCtx)
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadlineCtx.Done():
			return deadlineCtx.Err()
		case err := <-done:
			return err
		}
	}
}

// WithoutOverlap is a Laravel-inspired feature to ensure multiple jobs
// of a given key cannot run at the same time. This is useful in an example
// of where multiple jobs are touching the same source of data, such as
// a dollar amount, where the amount must must be decremented one at a time
// without race conditions.
func WithoutOverlap(job Job, key string, locker Locker[*sync.Mutex]) Job {
	return func(ctx context.Context) error {
		// Ensure a lock exists for this key.
		locker.Aquire(key, LockValue[*sync.Mutex]{
			ExpiresAt: time.Time{},
			Value:     &sync.Mutex{},
		})

		// Get the lock, whoever won the race will have the lock.
		lock, _ := locker.Get(key)
		mut := lock.Value
		mut.Lock()
		defer mut.Unlock()

		return job(ctx)
	}
}

// WithUnqiue is a Laravel-inspired feature which will ensure a job
// of a given key can must be unique for a duration of time (ut).
// If the duration of time has passed, the job will be allowed to fire.
// If the duration of time (ut) is zero, the lock will not expire until
// the job completes (useful for ensuring only one instance runs at a time).
// This could be useful in an example of where you want to ensure a
// notification email can only be sent once every 5 minutes.
// Note: Ideally the job would be kicked out before being pushed into
// the queue, however that would require the jobs to be more than just
// functions, it would require them to be structs with state, which
// adds overhead to the setup.
func WithUnique(job Job, key string, ut time.Duration, locker Locker[struct{}]) Job {
	return func(ctx context.Context) error {
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
		var expiresAt time.Time
		if ut == 0 {
			// Zero duration means no expiration (lock until job completes).
			expiresAt = time.Time{}
		} else {
			// Append duration to now.
			expiresAt = time.Now().Add(ut)
		}
		locker.Aquire(key, LockValue[struct{}]{
			ExpiresAt: expiresAt,
			Value:     es,
		})
		defer locker.Release(key)
		return job(ctx)
	}
}

// WithChain allows you to chain multiple jobs together where each
// job must complete before moving to the next. Should one of the
// jobs in the chain cause an error, the rest of the jobs will be
// discarded. If you would like to pass results from one job to the
// next, you can utilize a buffered channel or WithPipeline.
func WithChain(jobs ...Job) Job {
	return func(ctx context.Context) error {
		for _, job := range jobs {
			if err := job(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

// WithPipeline is identical in function to WithChain except
// it will create a buffered channel of a supplied type to
// pass into each job so that you do not have to create your own
// channel setup. Each job must be wrapped to accept the channel
// as a parameter.
func WithPipeline[T any](jobs ...func(chan T) Job) Job {
	results := make(chan T, 1)
	return func(ctx context.Context) error {
		for _, job := range jobs {
			if err := job(results)(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

// WithBatch wraps multiple jobs to track them as a single batch.
// When all jobs complete, onComplete is called.
// If any job fails, onFailure is called with all errors.
// The onProgress callback is optional and called after each job completes.
func WithBatch(jobs []Job, onComplete func(), onFailure func([]error), onProgress func(completed, total int)) ([]Job, *BatchState) {
	if len(jobs) == 0 {
		return nil, nil
	}

	state := &BatchState{
		TotalJobs:  int32(len(jobs)),
		Errors:     make([]error, 0),
		OnComplete: onComplete,
		OnFailure:  onFailure,
		OnProgress: onProgress,
	}

	wrappedJobs := make([]Job, len(jobs))
	for i, job := range jobs {
		wrappedJobs[i] = func(ojob Job) Job {
			return func(ctx context.Context) error {
				err := ojob(ctx)
				if err != nil {
					state.FailedJobs.Add(1)
					state.errorsMut.Lock()
					state.Errors = append(state.Errors, err)
					state.errorsMut.Unlock()
				}

				// Job ran without error, increment completed jobs.
				completed := state.CompletedJobs.Add(1)

				if state.OnProgress != nil {
					// Call onProgress callback, if provided.
					state.OnProgress(int(completed), int(state.TotalJobs))
				}

				// Check if this is the last job.
				if completed == state.TotalJobs {
					if state.FailedJobs.Load() > 0 && state.OnFailure != nil {
						// Call onFailure callback, if provided.
						state.OnFailure(state.Errors)
					} else if state.FailedJobs.Load() == 0 && state.OnComplete != nil {
						// Call onComplete callback, if provided.
						state.OnComplete()
					}
				}

				return err
			}
		}(job)
	}

	return wrappedJobs, state
}

// WithRelease returns a job back to the queue after a delay if it returns
// an error that matches the shouldRelease. This is useful when you want to
// re-enqueue a job after a certain amount of time if it fails (example, bad network call).
// The job will be released back to the queue and not marked as failed up to maxReleases times.
// After maxReleases is reached, the error will be returned and the job marked as failed.
// If maxReleases is 0, the job will be released forever.
func WithRelease(job Job, queue *Queue, delay time.Duration, maxReleases int, shouldRelease func(error) bool) Job {
	var releases int
	return func(ctx context.Context) error {
		err := job(ctx)
		if err != nil && shouldRelease(err) {
			if maxReleases == 0 || releases < maxReleases {
				releases++
				queue.DelayEnqueue(job, delay)
				return nil
			}
		}
		return err
	}
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

// WithTagged wraps a job with tags for tracking and cancellation purposes.
// The job is registered with the provided tags and can be cancelled
// using the registry's CancelForTag method.
// The job automatically unregisters itself when it completes or fails.
func WithTagged(job Job, registry *JobRegistry, tags ...string) Job {
	jobID := registry.NextID()
	return func(ctx context.Context) error {
		jobCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		registry.Register(jobID, tags, cancel)
		defer registry.Unregister(jobID, tags)

		return job(jobCtx)
	}
}

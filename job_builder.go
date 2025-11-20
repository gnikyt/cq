package cq

import (
	"sync"
	"time"
)

// Layer represents a function that wraps a job with additional behavior.
type Layer func(Job) Job

// JobBuilder allows for building jobs with multiple layers applied.
type JobBuilder struct {
	job    Job
	queue  *Queue
	layers []Layer
}

// NewJob creates a new JobBuilder for constructing a job.
func NewJob(job Job, queue *Queue) *JobBuilder {
	return &JobBuilder{
		job:    job,
		queue:  queue,
		layers: make([]Layer, 0),
	}
}

// Then adds a custom layer to the job.
// Layers are applied in reverse order.
func (jb *JobBuilder) Then(layer Layer) *JobBuilder {
	jb.layers = append(jb.layers, layer)
	return jb
}

// Build constructs the final job by applying all layers in reverse order.
func (jb *JobBuilder) Build() Job {
	job := jb.job
	for i := len(jb.layers) - 1; i >= 0; i-- {
		job = jb.layers[i](job)
	}
	return job
}

// Dispatch builds the job and enqueues it immediately.
func (jb *JobBuilder) Dispatch() {
	jb.queue.Enqueue(jb.Build())
}

// DispatchAfter builds the job and enqueues it after the specified delay.
func (jb *JobBuilder) DispatchAfter(delay time.Duration) {
	jb.queue.DelayEnqueue(jb.Build(), delay)
}

// TryDispatch builds the job and attempts to enqueue it without blocking.
// Returns true if the job was successfully enqueued, false if the queue is full.
func (jb *JobBuilder) TryDispatch() bool {
	return jb.queue.TryEnqueue(jb.Build())
}

// ThenRetry implements builder usage for WithRetry.
func (jb *JobBuilder) ThenRetry(times int) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithRetry(job, times)
	})
}

// ThenRetryWithBackoff implements builder usage for WithBackoff + WithRetry combined.
func (jb *JobBuilder) ThenRetryWithBackoff(times int, backoff BackoffFunc) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithBackoff(WithRetry(job, times), backoff)
	})
}

// ThenBackoff implements builder usage for WithBackoff.
func (jb *JobBuilder) ThenBackoff(backoff BackoffFunc) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithBackoff(job, backoff)
	})
}

// ThenUnique implements builder usage for WithUnique.
func (jb *JobBuilder) ThenUnique(key string, duration time.Duration, locker Locker[struct{}]) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithUnique(job, key, duration, locker)
	})
}

// ThenNoOverlap implements builder usage for WithoutOverlap.
func (jb *JobBuilder) ThenNoOverlap(key string, locker Locker[*sync.Mutex]) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithoutOverlap(job, key, locker)
	})
}

// ThenTimeout implements builder usage for WithTimeout.
func (jb *JobBuilder) ThenTimeout(timeout time.Duration) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithTimeout(job, timeout)
	})
}

// ThenDeadline implements builder usage for WithDeadline.
func (jb *JobBuilder) ThenDeadline(deadline time.Time) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithDeadline(job, deadline)
	})
}

// ThenRecover implements builder usage for WithRecover.
func (jb *JobBuilder) ThenRecover() *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithRecover(job)
	})
}

// ThenOnComplete implements builder usage for WithResultHandler.
func (jb *JobBuilder) ThenOnComplete(onSuccess func(), onFailure func(error)) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithResultHandler(job, onSuccess, onFailure)
	})
}

// ThenRelease implements builder usage for WithRelease.
func (jb *JobBuilder) ThenRelease(shouldRelease func(error) bool, delay time.Duration, maxReleases int) *JobBuilder {
	queue := jb.queue
	return jb.Then(func(job Job) Job {
		return WithRelease(job, queue, delay, maxReleases, shouldRelease)
	})
}

// ThenTag implements builder usage for WithTagged.
func (jb *JobBuilder) ThenTag(registry *JobRegistry, tags ...string) *JobBuilder {
	return jb.Then(func(job Job) Job {
		return WithTagged(job, registry, tags...)
	})
}

// ThenChainAfter implements builder usage for WithChain.
// Provided jobs will execute first before the current job.
func (jb *JobBuilder) ThenChainAfter(jobs ...Job) *JobBuilder {
	job := jb.job
	jb.job = WithChain(append(jobs, job)...)
	return jb
}

// ThenChainBefore implements builder usage for WithChain.
// Provided jobs will execute first before the current job.
func (jb *JobBuilder) ThenChainBefore(jobs ...Job) *JobBuilder {
	job := jb.job
	jb.job = WithChain(append([]Job{job}, jobs...)...)
	return jb
}

// ScheduleEvery builds the job and schedules it to run at the specified interval.
// Returns an error if a job with the same ID already exists in the scheduler.
func (jb *JobBuilder) ScheduleEvery(scheduler *Scheduler, id string, interval time.Duration) error {
	return scheduler.Every(id, interval, jb.Build())
}

// ScheduleAt builds the job and schedules it to run once at the specified time.
// Returns an error if a job with the same ID already exists in the scheduler.
func (jb *JobBuilder) ScheduleAt(scheduler *Scheduler, id string, t time.Time) error {
	return scheduler.At(id, t, jb.Build())
}

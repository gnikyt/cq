package cq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// JobRegistry tracks jobs by tags for monitoring and cancellation purposes.
// Allowing us to "track" jobs by tagging them.
// Jobs can be tagged and later cancelled by tag, useful for batch
// operations like cancelling all jobs for a specific resource.
// Jobs can have multiple tags, and can be cancelled by any of the tags.
type JobRegistry struct {
	jobs   map[string]map[string]context.CancelFunc // tag -> jobID -> cancelFunc
	mut    sync.RWMutex                             // Mutex for protecting the jobs map.
	nextID atomic.Int64                             // Atomic counter for generating unique job IDs.
}

// NewJobRegistry creates a new JobRegistry for tracking tagged jobs.
func NewJobRegistry() *JobRegistry {
	return &JobRegistry{
		jobs: make(map[string]map[string]context.CancelFunc),
	}
}

// Register adds a job to the registry with the given tags.
// The jobID uniquely identifies the job, and the cancel function
// allows the job to be cancelled when needed.
func (jr *JobRegistry) Register(jobID string, tags []string, cancelCtx context.CancelFunc) {
	jr.mut.Lock()
	defer jr.mut.Unlock()

	for _, tag := range tags {
		if _, exists := jr.jobs[tag]; !exists {
			jr.jobs[tag] = make(map[string]context.CancelFunc)
		}
		jr.jobs[tag][jobID] = cancelCtx
	}
}

// Unregister removes a job from the registry for all its tags.
// This should be called when a job completes or fails.
func (jr *JobRegistry) Unregister(jobID string, tags []string) {
	jr.mut.Lock()
	defer jr.mut.Unlock()

	for _, tag := range tags {
		if jobs, exists := jr.jobs[tag]; exists {
			delete(jobs, jobID)
			if len(jobs) == 0 {
				// Tag is now empty, remove it.
				delete(jr.jobs, tag)
			}
		}
	}
}

// CancelForTag cancels all jobs with the given tag.
// Returns the number of jobs that were cancelled.
func (jr *JobRegistry) CancelForTag(tag string) int {
	jr.mut.Lock()
	defer jr.mut.Unlock()

	count := 0
	if jobs, exists := jr.jobs[tag]; exists {
		for _, cancel := range jobs {
			cancel()
			count++
		}
		delete(jr.jobs, tag)
	}
	return count
}

// CancelAll cancels all jobs in the registry.
// Returns the number of jobs that were cancelled.
func (jr *JobRegistry) CancelAll() int {
	jr.mut.Lock()
	defer jr.mut.Unlock()

	count := 0
	for tag, jobs := range jr.jobs {
		for _, cancel := range jobs {
			cancel()
			count++
		}
		delete(jr.jobs, tag)
	}
	return count
}

// CountForTag returns the number of jobs currently registered with the given tag.
func (jr *JobRegistry) CountForTag(tag string) int {
	jr.mut.RLock()
	defer jr.mut.RUnlock()

	if jobs, exists := jr.jobs[tag]; exists {
		return len(jobs)
	}
	return 0
}

// Tags returns all currently registered tags.
func (jr *JobRegistry) Tags() []string {
	jr.mut.RLock()
	defer jr.mut.RUnlock()

	tags := make([]string, 0, len(jr.jobs))
	for tag := range jr.jobs {
		tags = append(tags, tag)
	}
	return tags
}

// NextID generates a unique job ID.
func (jr *JobRegistry) NextID() string {
	id := jr.nextID.Add(1)
	return fmt.Sprintf("job-%d", id)
}

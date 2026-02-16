package cq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Scheduler errors.
var (
	ErrJobExists       = errors.New("job with id already exists")
	ErrInvalidInterval = errors.New("interval must be positive")
	ErrScheduleInPast  = errors.New("scheduled time is in the past")
)

// Scheduler manages recurring and one-time jobs.
// Jobs are enqueued into the provided queue when schedules trigger.
type Scheduler struct {
	queue     *Queue             // Queue to enqueue jobs into.
	ctx       context.Context    // Context for scheduler lifecycle.
	ctxCancel context.CancelFunc // Cancel function for explicit Stop().

	wg sync.WaitGroup // Wait group.

	mu   sync.RWMutex             // Mutex for scheduler jobs map.
	jobs map[string]*scheduledJob // Map of job ID to scheduled job.
}

// scheduledJob represents a scheduled job.
type scheduledJob struct {
	id        string             // Job ID.
	job       Job                // Job to execute.
	interval  time.Duration      // Interval for recurring jobs (0 for one-time).
	runAt     time.Time          // Run time for one-time jobs.
	ctxCancel context.CancelFunc // Cancel function for this job.
}

// NewScheduler creates a new Scheduler that enqueues jobs into the provided queue.
// Jobs start immediately when added (no Start() call needed).
// The scheduler will automatically stop when the provided context is cancelled.
// Call Stop() for explicit shutdown and to wait for all goroutines to finish.
func NewScheduler(ctx context.Context, queue *Queue) *Scheduler {
	childCtx, cancel := context.WithCancel(ctx)
	return &Scheduler{
		queue:     queue,
		jobs:      make(map[string]*scheduledJob),
		ctx:       childCtx,
		ctxCancel: cancel,
	}
}

// Stop stops scheduled jobs and waits for scheduler goroutines to exit.
func (s *Scheduler) Stop() {
	s.ctxCancel()
	s.wg.Wait()
}

// Every schedules a recurring job at the specified interval.
// The job is enqueued each time the interval ticks.
// Returns an error if a job with the same ID already exists.
func (s *Scheduler) Every(id string, interval time.Duration, job Job) error {
	if interval <= 0 {
		return fmt.Errorf("scheduler: every: %w", ErrInvalidInterval)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[id]; exists {
		return fmt.Errorf("scheduler: every (id=%s): %w", id, ErrJobExists)
	}

	ctx, cancel := context.WithCancel(s.ctx)
	sj := &scheduledJob{
		id:        id,
		job:       job,
		interval:  interval,
		ctxCancel: cancel,
	}

	s.jobs[id] = sj
	s.wg.Add(1)

	go s.runRecurring(ctx, sj)

	return nil
}

// At schedules a one-time job for the specified time.
// The job is enqueued once and then removed from the scheduler.
// Returns an error if a job with the same ID already exists or if the time is in the past.
func (s *Scheduler) At(id string, t time.Time, job Job) error {
	if time.Now().After(t) {
		return fmt.Errorf("scheduler: at: %w", ErrScheduleInPast)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[id]; exists {
		return fmt.Errorf("scheduler: at (id=%s): %w", id, ErrJobExists)
	}

	ctx, cancel := context.WithCancel(s.ctx)
	sj := &scheduledJob{
		id:        id,
		job:       job,
		runAt:     t,
		ctxCancel: cancel,
	}

	s.jobs[id] = sj
	s.wg.Add(1)

	go s.runOnce(ctx, sj)

	return nil
}

// Remove cancels and removes a scheduled job by ID.
// Returns true if the job was found and removed, false otherwise.
func (s *Scheduler) Remove(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	sj, exists := s.jobs[id]
	if !exists {
		return false
	}

	sj.ctxCancel()
	delete(s.jobs, id)

	return true
}

// Has reports whether a job with ID exists.
func (s *Scheduler) Has(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.jobs[id]
	return exists
}

// Count returns the number of scheduled jobs.
func (s *Scheduler) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.jobs)
}

// List returns all scheduled job IDs.
func (s *Scheduler) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	return ids
}

// runRecurring runs the recurring scheduling loop for one job.
func (s *Scheduler) runRecurring(ctx context.Context, sj *scheduledJob) {
	defer s.wg.Done()

	ticker := time.NewTicker(sj.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return // Context cancelled, stop the job.
		case <-ticker.C:
			s.queue.Enqueue(sj.job) // Enqueue the job.
		}
	}
}

// runOnce waits until runAt, enqueues the job once, then removes it.
func (s *Scheduler) runOnce(ctx context.Context, sj *scheduledJob) {
	defer s.wg.Done()

	delay := time.Until(sj.runAt)
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return // Context cancelled, stop the job.
	case <-timer.C:
		s.queue.Enqueue(sj.job)
		s.Remove(sj.id)
	}
}

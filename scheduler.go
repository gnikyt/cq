package cq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Scheduler errors.
var (
	ErrScheduleExists          = errors.New("schedule with id already exists")
	ErrScheduleIntervalInvalid = errors.New("schedule interval must be positive")
	ErrScheduleInPast          = errors.New("scheduled time is in the past")
	ErrSchedulerStopped        = errors.New("scheduler is stopped")
	ErrScheduleJobRequired     = errors.New("schedule job required")
	ErrScheduleRequired        = errors.New("schedule required")
)

// ScheduleHandle tracks one recurring or one-time schedule.
type ScheduleHandle struct {
	id     string      // Schedule identifier.
	cancel func() bool // Cancels this schedule instance.

	submissionAttempts atomic.Uint64 // Number of queue submission attempts.
	done               chan struct{} // Closed when the schedule becomes terminal.
	doneOnce           sync.Once     // Ensures done closes once.

	mu        sync.RWMutex // Guards latest submission data.
	latest    *JobHandle   // Latest accepted submission.
	latestErr error        // Latest submission rejection.
}

// ID returns the schedule ID.
func (h *ScheduleHandle) ID() string {
	return h.id
}

// Cancel removes the schedule and prevents future submission attempts.
// It does not cancel jobs already accepted by the queue.
func (h *ScheduleHandle) Cancel() bool {
	if h.cancel == nil {
		return false
	}
	return h.cancel()
}

// Done returns a channel closed when the schedule is removed, completes, or stops.
func (h *ScheduleHandle) Done() <-chan struct{} {
	return h.done
}

// SubmissionAttempts returns the number of submission attempts made by the schedule.
func (h *ScheduleHandle) SubmissionAttempts() uint64 {
	return h.submissionAttempts.Load()
}

// Latest returns the latest submission attempt.
// attempted is false before the first submission attempt.
func (h *ScheduleHandle) Latest() (handle *JobHandle, err error, attempted bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.submissionAttempts.Load() == 0 {
		return nil, nil, false
	}
	return h.latest, h.latestErr, true
}

// recordSubmission stores the latest submission attempt.
func (h *ScheduleHandle) recordSubmission(handle *JobHandle, err error) {
	h.mu.Lock()
	h.latest = handle
	h.latestErr = err
	h.submissionAttempts.Add(1)
	h.mu.Unlock()
}

// finish marks the schedule terminal, reporting whether this call did it.
func (h *ScheduleHandle) finish() bool {
	finished := false
	h.doneOnce.Do(func() {
		close(h.done)
		finished = true
	})
	return finished
}

// SchedulerHooks are optional callbacks for schedule lifecycle events.
// Callbacks must be safe for concurrent use and should not block.
type SchedulerHooks struct {
	// OnFire runs after every submission attempt with the schedule ID,
	// the accepted JobHandle (nil when rejected), and the rejection error.
	OnFire func(id string, handle *JobHandle, err error)
	// OnComplete runs once when a schedule becomes terminal
	// (cancelled, removed, or self-completed).
	OnComplete func(id string)
}

// SchedulerOption configures optional scheduler behavior.
type SchedulerOption func(*Scheduler)

// WithSchedulerHooks registers scheduler lifecycle hooks.
// It can be passed multiple times... all registered hooks are executed.
func WithSchedulerHooks(hooks SchedulerHooks) SchedulerOption {
	return func(s *Scheduler) {
		s.hooks = append(s.hooks, hooks)
	}
}

// Scheduler manages recurring and one-time job submissions.
type Scheduler struct {
	queue     *Queue             // Queue receiving scheduled submissions.
	ctx       context.Context    // Scheduler lifecycle context.
	ctxCancel context.CancelFunc // Stops the scheduler without cancelling its parent.

	wg sync.WaitGroup // Tracks schedule goroutines.

	mu      sync.RWMutex             // Guards schedules and stopped.
	jobs    map[string]*scheduledJob // Registered schedules by ID.
	stopped bool                     // Indicates explicit scheduler shutdown.

	hooks []SchedulerHooks // Optional lifecycle callbacks.
}

// scheduledJob contains one registered schedule.
type scheduledJob struct {
	job      Job                // Job submitted when the schedule fires.
	interval time.Duration      // Recurring interval... zero for one-time schedules.
	runAt    time.Time          // One-time schedule timestamp.
	schedule Schedule           // Optional Schedule implementation driving fire times.
	opts     []SubmitOption     // Options applied to each submission.
	ctx      context.Context    // Per-schedule lifecycle context.
	cancel   context.CancelFunc // Cancels this schedule only.
	handle   *ScheduleHandle    // Public schedule lifecycle handle.
}

// NewScheduler creates a Scheduler that submits jobs into queue.
// Schedules start immediately when added and stop when ctx is cancelled.
func NewScheduler(ctx context.Context, queue *Queue, opts ...SchedulerOption) *Scheduler {
	if queue == nil {
		panic("cq: scheduler queue required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Create a child context with a cancel function.
	childCtx, cancel := context.WithCancel(ctx)

	// Create a new scheduler.
	s := &Scheduler{
		queue:     queue,
		jobs:      make(map[string]*scheduledJob),
		ctx:       childCtx,
		ctxCancel: cancel,
	}

	// Apply functional options.
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Stop stops all schedules and waits for scheduler goroutines to exit.
// It does not cancel jobs already accepted by the queue.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.ctxCancel()
	s.mu.Unlock()
	s.wg.Wait()
}

// Every registers a recurring schedule.
func (s *Scheduler) Every(id string, interval time.Duration, job Job, opts ...SubmitOption) (*ScheduleHandle, error) {
	if job == nil {
		return nil, fmt.Errorf("scheduler: every: %w", ErrScheduleJobRequired)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("scheduler: every: %w", ErrScheduleIntervalInvalid)
	}
	return s.add(id, job, interval, time.Time{}, nil, opts)
}

// On registers a recurring schedule whose fire times are computed by a
// Schedule implementation, such as a cron expression via ParseCron.
// The schedule ends when Next returns the zero time or a non-advancing time.
func (s *Scheduler) On(id string, schedule Schedule, job Job, opts ...SubmitOption) (*ScheduleHandle, error) {
	if job == nil {
		return nil, fmt.Errorf("scheduler: on: %w", ErrScheduleJobRequired)
	}
	if schedule == nil {
		return nil, fmt.Errorf("scheduler: on: %w", ErrScheduleRequired)
	}
	return s.add(id, job, 0, time.Time{}, schedule, opts)
}

// At registers a one-time schedule.
func (s *Scheduler) At(id string, at time.Time, job Job, opts ...SubmitOption) (*ScheduleHandle, error) {
	if job == nil {
		return nil, fmt.Errorf("scheduler: at: %w", ErrScheduleJobRequired)
	}
	if time.Now().After(at) {
		return nil, fmt.Errorf("scheduler: at: %w", ErrScheduleInPast)
	}
	return s.add(id, job, 0, at, nil, opts)
}

// Remove cancels and removes a schedule by ID.
func (s *Scheduler) Remove(id string) bool {
	s.mu.RLock()
	sj, exists := s.jobs[id]
	s.mu.RUnlock()
	if !exists {
		return false
	}
	return s.remove(sj)
}

// Has reports whether a schedule with ID exists.
func (s *Scheduler) Has(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.jobs[id]
	return exists
}

// Count returns the number of registered schedules.
func (s *Scheduler) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.jobs)
}

// List returns all registered schedule IDs.
func (s *Scheduler) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	return ids
}

// add registers and starts one schedule.
func (s *Scheduler) add(id string, job Job, interval time.Duration, at time.Time, schedule Schedule, opts []SubmitOption) (*ScheduleHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, ErrSchedulerStopped
	}
	select {
	case <-s.ctx.Done():
		return nil, ErrSchedulerStopped
	default:
	}
	if _, exists := s.jobs[id]; exists {
		return nil, fmt.Errorf("scheduler: add (id=%s): %w", id, ErrScheduleExists)
	}

	ctx, cancel := context.WithCancel(s.ctx)
	handle := &ScheduleHandle{id: id, done: make(chan struct{})}
	sj := &scheduledJob{
		job:      job,
		interval: interval,
		runAt:    at,
		schedule: schedule,
		opts:     append([]SubmitOption(nil), opts...),
		ctx:      ctx,
		cancel:   cancel,
		handle:   handle,
	}
	handle.cancel = func() bool { return s.remove(sj) }
	s.jobs[id] = sj
	s.wg.Add(1)
	switch {
	case schedule != nil:
		go s.runSchedule(sj)
	case interval > 0:
		go s.runRecurring(sj)
	default:
		go s.runOnce(sj)
	}
	return handle, nil
}

// remove removes a schedule only if it is still the registered instance.
func (s *Scheduler) remove(sj *scheduledJob) bool {
	s.mu.Lock()
	current, exists := s.jobs[sj.handle.id]
	if exists && current == sj {
		delete(s.jobs, sj.handle.id)
	}
	s.mu.Unlock()
	if !exists || current != sj {
		return false
	}
	sj.cancel()
	if sj.handle.finish() {
		s.notifyComplete(sj.handle.id)
	}
	return true
}

// removeCompleted removes a schedule only if it is still the registered instance.
func (s *Scheduler) removeCompleted(sj *scheduledJob) {
	s.mu.Lock()
	if current, exists := s.jobs[sj.handle.id]; exists && current == sj {
		delete(s.jobs, sj.handle.id)
	}
	s.mu.Unlock()
	sj.cancel()
	if sj.handle.finish() {
		s.notifyComplete(sj.handle.id)
	}
}

// submit records one schedule-triggered submission attempt.
func (s *Scheduler) submit(sj *scheduledJob) {
	handle, err := s.queue.Submit(sj.ctx, sj.job, sj.opts...)
	sj.handle.recordSubmission(handle, err)
	for _, hooks := range s.hooks {
		if hooks.OnFire != nil {
			hooks.OnFire(sj.handle.id, handle, err)
		}
	}
}

// notifyComplete runs OnComplete hooks for a terminal schedule.
func (s *Scheduler) notifyComplete(id string) {
	for _, hooks := range s.hooks {
		if hooks.OnComplete != nil {
			hooks.OnComplete(id)
		}
	}
}

// runRecurring runs one recurring schedule.
func (s *Scheduler) runRecurring(sj *scheduledJob) {
	defer s.wg.Done()
	defer s.removeCompleted(sj)

	ticker := time.NewTicker(sj.interval)
	defer ticker.Stop()
	for {
		select {
		case <-sj.ctx.Done():
			return
		case <-ticker.C:
			s.submit(sj)
		}
	}
}

// runSchedule runs one Schedule-driven recurring schedule.
// The schedule completes when Next returns the zero time or fails to advance.
func (s *Scheduler) runSchedule(sj *scheduledJob) {
	defer s.wg.Done()
	defer s.removeCompleted(sj)

	for {
		now := time.Now()
		next := sj.schedule.Next(now)
		if next.IsZero() || !next.After(now) {
			return // No future fire time... schedule is complete.
		}

		timer := time.NewTimer(time.Until(next))
		select {
		case <-sj.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			s.submit(sj)
		}
	}
}

// runOnce waits until runAt, submits once, then removes the schedule.
func (s *Scheduler) runOnce(sj *scheduledJob) {
	defer s.wg.Done()
	defer s.removeCompleted(sj)

	timer := time.NewTimer(time.Until(sj.runAt))
	defer timer.Stop()

	select {
	case <-sj.ctx.Done():
		return
	case <-timer.C:
		s.submit(sj)
	}
}

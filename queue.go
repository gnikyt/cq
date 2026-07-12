package cq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultWorkerIdleTick = 5 * time.Second       // Every five seconds check for idle workers.
	defaultPausePollTick  = 1 * time.Second       // Every second sync distributed pause state.
	defaultPauseWaitTick  = 25 * time.Millisecond // Worker pause check interval.
)

// Queue submission errors.
var (
	ErrQueueStopped     = errors.New("queue is stopped")
	ErrQueuePaused      = errors.New("queue is paused")
	ErrQueueFull        = errors.New("queue is full")
	ErrQueueJobRequired = errors.New("queue: job required")
	ErrQueueDrained     = errors.New("queue drained")
)

// queuedJob is one buffered submission awaiting a worker.
type queuedJob struct {
	run    Job        // Wrapped execution closure.
	raw    Job        // Job as submitted, before queue middleware, for drain handback.
	handle *JobHandle // Submission handle.
}

// DrainedJob is a submission handed back by StopDrain before it started executing.
// It carries everything needed to persist or resubmit the work elsewhere.
type DrainedJob struct {
	Job  Job     // The job as submitted (wrappers intact, queue middleware not applied).
	Meta JobMeta // Submission metadata (ID, name, attributes, enqueue time).
}

// IDGenerator creates a job ID for accepted submissions.
type IDGenerator func() string

// submissionOptions configures internal submission acceptance.
type submissionOptions struct {
	blocking  bool            // Whether to block if no worker can be started and the jobs channel is full.
	acceptCtx context.Context // Cancels waiting for acceptance without cancelling an accepted job.
	meta      JobMeta         // Metadata for the accepted submission.
	handle    *JobHandle      // Completion handle for the accepted submission.
}

// QueueStats is an atomic snapshot of queue state and tallies.
type QueueStats struct {
	Name    string
	Stopped bool
	Paused  bool

	WorkersMin     int
	WorkersMax     int
	RunningWorkers int
	IdleWorkers    int
	Capacity       int

	CreatedJobs   int
	PendingJobs   int
	ActiveJobs    int
	FailedJobs    int
	DiscardedJobs int
	CancelledJobs int
	CompletedJobs int

	RescheduledJobs int
	ReleasedJobs    int
}

// Queue dispatches jobs to workers.
// It dynamically scales workers between configured minimum and maximum limits,
// and tracks runtime job and worker metrics.
type Queue struct {
	ctx       context.Context    // Queue context for workers/jobs.
	ctxCancel context.CancelFunc // Cancels the queue context.
	stopped   atomic.Bool        // Indicates queue shutdown has started.
	started   atomic.Bool        // Indicates queue start has run.

	workersMin          int            // Minimum worker count.
	workersMax          int            // Maximum worker count.
	workerIdleTick      time.Duration  // Interval for idle-worker cleanup.
	workerWg            sync.WaitGroup // Tracks worker and cleanup goroutines.
	mut                 sync.Mutex     // Guards worker scaling decisions.
	workersRunningTally atomic.Int32   // Reserved/active worker slots used for scaling decisions.
	workersIdleTally    atomic.Int32   // Reserved idle worker slots available for new jobs.

	jobs          chan *queuedJob // Buffered job queue.
	jobWg         sync.WaitGroup  // Tracks accepted jobs.
	acceptMut     sync.RWMutex    // Synchronizes submission acceptance with Stop/Terminate.
	jobsCloseOnce sync.Once       // Ensures jobs channel is closed at most once.

	createdJobsTally     atomic.Int64 // Total jobs accepted.
	activeJobsTally      atomic.Int64 // Jobs currently executing.
	pendingJobsTally     atomic.Int64 // Jobs waiting in the queue.
	failedJobsTally      atomic.Int64 // Jobs completed with error.
	cancelledJobsTally   atomic.Int64 // Jobs completed through handle cancellation.
	completedJobsTally   atomic.Int64 // Jobs completed successfully.
	discardedJobsTally   atomic.Int64 // Jobs completed as discarded outcomes.
	rescheduledJobsTally atomic.Int64 // Total job reschedule requests.
	releasedJobsTally    atomic.Int64 // Total release/release-self requests.

	jobIDCounter   atomic.Int64            // Counter for generating unique job IDs.
	idGenerator    IDGenerator             // Optional override for generating job IDs.
	submissionsMut sync.Mutex              // Guards unresolved submissions.
	submissions    map[*JobHandle]struct{} // Accepted submissions that are not terminal.
	delayedJobs    map[*JobHandle]Job      // Delayed submissions awaiting their timer, for drain handback.

	panicHandler    func(any)    // Optional panic handler for job panics.
	middleware      []Middleware // Optional queue-level middleware chain.
	hooks           []Hooks      // Optional lifecycle hooks for queue transitions.
	hasAttemptHooks bool         // Whether any registered hook listens for attempt events.

	paused        atomic.Bool   // Local pause state.
	distPaused    atomic.Bool   // Distributed pause state from store polling.
	pauseStore    PauseStore    // Optional distributed pause state store.
	pauseStoreKey string        // Key used for distributed pause state.
	pausePollTick time.Duration // Interval for distributed pause polling.
	pauseBehavior PauseBehavior // Submission behavior while paused.
	name          string        // Optional queue name used for observability.
}

// NewQueue creates a queue with worker and buffer limits.
// `wmin` is the minimum worker count, `wmax` is the maximum worker count,
// and `cap` is the jobs channel capacity. Optional settings can be passed
// via `opts`.
//
// Panics if wmin < 0, wmax < wmin, or cap < 0.
func NewQueue(wmin int, wmax int, cap int, opts ...QueueOption) *Queue {
	if wmin < 0 {
		panic("cq: wmin must be >= 0")
	}
	if wmax < wmin {
		panic("cq: wmax must be >= wmin")
	}
	if cap < 0 {
		panic("cq: cap must be >= 0")
	}

	q := &Queue{
		workersMin:     wmin,
		workersMax:     wmax,
		jobs:           make(chan *queuedJob, cap),
		jobWg:          sync.WaitGroup{},
		workerWg:       sync.WaitGroup{},
		workerIdleTick: defaultWorkerIdleTick,
		pausePollTick:  defaultPausePollTick,
		pauseBehavior:  PauseBuffer,
		submissions:    make(map[*JobHandle]struct{}),
		delayedJobs:    make(map[*JobHandle]Job),
	}

	// Apply functional options.
	for _, opt := range opts {
		opt(q)
	}

	if q.ctx == nil {
		// Default to use a background context.
		WithContext(context.Background())(q)
	}

	return q
}

// WorkerRange returns the minimum and maximum workers configured.
func (q *Queue) WorkerRange() (int, int) {
	q.mut.Lock()
	defer q.mut.Unlock()
	return q.workersMin, q.workersMax
}

// SetWorkerRange updates the minimum and maximum workers at runtime.
// It starts workers immediately when min increases.
// It does not affect running workers when max decreases... idle cleanup will drain excess workers.
func (q *Queue) SetWorkerRange(min int, max int) error {
	if min < 0 {
		return fmt.Errorf("queue: min workers must be >= 0")
	}
	if max < min {
		return fmt.Errorf("queue: max workers must be >= min workers")
	}
	if q.IsStopped() {
		return ErrQueueStopped
	}

	toStart := 0

	q.mut.Lock()
	if q.stopped.Load() {
		q.mut.Unlock()
		return ErrQueueStopped
	}

	q.workersMin = min
	q.workersMax = max

	running := int(q.workersRunningTally.Load())
	if running < q.workersMin {
		toStart = q.workersMin - running
	}
	q.mut.Unlock()

	for range toStart {
		// Best-effort pre-warm to satisfy the updated minimum.
		_ = q.newWorker(nil)
	}

	return nil
}

// Capacity returns the capacity of the jobs channel.
func (q *Queue) Capacity() int {
	return cap(q.jobs)
}

// Start begins the idle-worker ticker and starts the configured minimum workers.
func (q *Queue) Start() {
	if q.IsStopped() {
		return // Queue is stopped.
	}
	if !q.started.CompareAndSwap(false, true) {
		return // Already started.
	}

	// Start the idle-worker ticker.
	q.workerWg.Add(1)
	go q.cleanupIdleWorkers()

	if q.pauseStore != nil {
		// Start the distributed pause poller.
		q.workerWg.Add(1)
		go q.pollDistributedPause()
	}

	// Start the minimum number of workers.
	for range q.workersMin {
		q.newWorker(nil)
	}
}

// Stop gracefully shuts down the queue.
// It marks the queue as stopped, optionally waits for queued jobs to finish
// when `jobWait` is true, waits for worker goroutines to exit, resets worker
// tallies, and closes the jobs channel.
func (q *Queue) Stop(jobWait bool) {
	q.acceptMut.Lock()
	defer q.acceptMut.Unlock()

	q.stopped.Store(true)
	if jobWait {
		// Background context... the wait is unbounded.
		_ = q.waitForShutdown(context.Background())
		return
	}
	q.abandonPendingSubmissions()
	q.ctxCancel()
	q.workerWg.Wait()
	q.resetWorkers()
	q.paused.Store(false)
	q.distPaused.Store(false)
	q.started.Store(false)
	q.closeJobs()
}

// StopContext gracefully shuts down the queue while bounded by ctx.
// It behaves like Stop(true) unless ctx is done before queued/in-flight jobs finish.
// On timeout/cancel, it cancels queue context and closes the queue without waiting
// for worker goroutines to exit.
func (q *Queue) StopContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background() // Default to background context.
	}

	if q.IsStopped() {
		return ErrQueueStopped
	}

	q.acceptMut.Lock()
	defer q.acceptMut.Unlock()

	if q.IsStopped() {
		return ErrQueueStopped
	}

	q.stopped.Store(true)
	return q.waitForShutdown(ctx)
}

// StopTimeout gracefully shuts down the queue for up to tt.
// It is equivalent to calling StopContext with a timeout context.
func (q *Queue) StopTimeout(tt time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), tt)
	defer cancel()
	return q.StopContext(ctx)
}

// StopDrain gracefully shuts down the queue and hands back jobs that never
// started executing (buffered or delayed) as DrainedJob values, so callers
// can persist or re-route unstarted work. Handed-back handles resolve with
// ErrQueueDrained and their tallies are removed as if never accepted.
// In-flight jobs run to completion bounded by ctx: like StopContext, a done
// ctx abandons the wait and returns ctx.Err alongside jobs drained so far.
func (q *Queue) StopDrain(ctx context.Context) ([]DrainedJob, error) {
	if ctx == nil {
		ctx = context.Background() // Default to background context.
	}

	if q.IsStopped() {
		return nil, ErrQueueStopped
	}

	q.acceptMut.Lock()
	defer q.acceptMut.Unlock()

	if q.IsStopped() {
		return nil, ErrQueueStopped
	}
	q.stopped.Store(true)

	var drained []DrainedJob

	// Hand back buffered jobs that no worker picked up.
	// Receiving competes with workers, but each item goes to exactly one side.
buffered:
	for {
		select {
		case item := <-q.jobs:
			if item == nil {
				continue // Idle-stop sentinel... skip.
			}

			if item.handle.rejectPending(ErrQueueDrained) {
				drained = append(drained, DrainedJob{Job: item.raw, Meta: item.handle.Meta()})
				q.rollbackJobEnqueued()
			} else {
				// Already terminal (example: cancelled while buffered)... release accounting only.
				q.pendingJobsTally.Add(-1)
				q.cancelledJobsTally.Add(1)
			}
			q.untrackSubmission(item.handle)
			q.jobWg.Done()
		default:
			break buffered
		}
	}

	// Hand back delayed submissions still waiting on their timer.
	q.submissionsMut.Lock()
	for handle, job := range q.delayedJobs {
		if handle.rejectPending(ErrQueueDrained) {
			drained = append(drained, DrainedJob{Job: job, Meta: handle.Meta()})
		}
		delete(q.delayedJobs, handle)
		delete(q.submissions, handle)
	}
	q.submissionsMut.Unlock()

	// In-flight jobs finish bounded by ctx... unstarted work was handed back.
	return drained, q.waitForShutdown(ctx)
}

// waitForShutdown waits for accepted jobs to finish bounded by ctx, then
// completes queue shutdown. On ctx done, pending submissions are abandoned
// and worker cleanup finishes asynchronously. Callers must hold acceptMut
// and have set stopped.
func (q *Queue) waitForShutdown(ctx context.Context) error {
	// Ensure graceful shutdown can drain pending jobs.
	q.paused.Store(false)
	q.distPaused.Store(false)

	done := make(chan struct{}, 1)
	go func() {
		q.jobWg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		q.ctxCancel()
		q.workerWg.Wait()
		q.resetWorkers()
		q.paused.Store(false)
		q.distPaused.Store(false)
		q.started.Store(false)
		q.closeJobs()
		return nil
	case <-ctx.Done():
		q.ctxCancel()
		q.abandonPendingSubmissions()
		// Re-clear pause flags... a concurrent Pause() may have set them during the wait.
		q.paused.Store(false)
		q.distPaused.Store(false)
		go func() {
			q.workerWg.Wait()
			q.resetWorkers()
			q.started.Store(false)
			q.closeJobs()
		}()
		return ctx.Err()
	}
}

// Terminate forces an immediate shutdown.
// Unlike Stop, it does not wait for jobs or worker goroutines to finish.
func (q *Queue) Terminate() {
	q.acceptMut.Lock()
	defer q.acceptMut.Unlock()

	q.stopped.Store(true)
	q.abandonPendingSubmissions()
	q.ctxCancel()
	q.paused.Store(false)
	q.distPaused.Store(false)
	q.started.Store(false)
	go func() {
		q.workerWg.Wait()
		q.resetWorkers()
		q.closeJobs()
	}()
}

// IsStopped atomically checks if the queue is stopped.
func (q *Queue) IsStopped() bool {
	return q.stopped.Load()
}

// Pause prevents new jobs from starting execution.
// Submissions are still accepted, and running jobs continue.
func (q *Queue) Pause() error {
	q.acceptMut.RLock()
	defer q.acceptMut.RUnlock()

	if q.IsStopped() {
		return ErrQueueStopped
	}

	q.paused.Store(true)
	if q.pauseStore == nil || q.pauseStoreKey == "" {
		return nil // No distributed pause store, local pause only.
	}

	if err := q.pauseStore.SetPaused(q.ctx, q.pauseStoreKey, true); err != nil {
		return fmt.Errorf("queue: pause: %w", err)
	}
	return nil
}

// Resume allows new jobs to start execution again.
func (q *Queue) Resume() error {
	q.acceptMut.RLock()
	defer q.acceptMut.RUnlock()

	if q.IsStopped() {
		return ErrQueueStopped
	}

	q.paused.Store(false)
	if q.pauseStore == nil || q.pauseStoreKey == "" {
		return nil // No distributed pause store, local resume only.
	}

	if err := q.pauseStore.SetPaused(q.ctx, q.pauseStoreKey, false); err != nil {
		return fmt.Errorf("queue: resume: %w", err)
	}
	return nil
}

// IsPaused reports whether queue execution is currently paused.
func (q *Queue) IsPaused() bool {
	return q.paused.Load() || q.distPaused.Load()
}

// TallyOf atomically returns the number of jobs for a given state.
func (q *Queue) TallyOf(js JobState) int {
	var val int64
	switch js {
	case JobStateCompleted:
		val = q.completedJobsTally.Load()
	case JobStateFailed:
		val = q.failedJobsTally.Load()
	case JobStateCancelled:
		val = q.cancelledJobsTally.Load()
	case JobStateDiscarded:
		val = q.discardedJobsTally.Load()
	case JobStatePending:
		val = q.pendingJobsTally.Load()
	case JobStateActive:
		val = q.activeJobsTally.Load()
	default:
		val = q.createdJobsTally.Load()
	}
	return int(val)
}

// RunningWorkers atomically returns the number of running workers.
func (q *Queue) RunningWorkers() int {
	return int(q.workersRunningTally.Load())
}

// IdleWorkers atomically returns the number of idle workers.
func (q *Queue) IdleWorkers() int {
	return int(q.workersIdleTally.Load())
}

// Stats returns a snapshot of queue state, worker counts, and job tallies.
// This is intended for metrics/observability. It does not provide transactional
// consistency across every field.
func (q *Queue) Stats() QueueStats {
	q.mut.Lock()
	wmin := q.workersMin
	wmax := q.workersMax
	q.mut.Unlock()

	return QueueStats{
		Name:            q.name,
		Stopped:         q.stopped.Load(),
		Paused:          q.IsPaused(),
		WorkersMin:      wmin,
		WorkersMax:      wmax,
		RunningWorkers:  int(q.workersRunningTally.Load()),
		IdleWorkers:     int(q.workersIdleTally.Load()),
		Capacity:        cap(q.jobs),
		CreatedJobs:     int(q.createdJobsTally.Load()),
		PendingJobs:     int(q.pendingJobsTally.Load()),
		ActiveJobs:      int(q.activeJobsTally.Load()),
		FailedJobs:      int(q.failedJobsTally.Load()),
		DiscardedJobs:   int(q.discardedJobsTally.Load()),
		CancelledJobs:   int(q.cancelledJobsTally.Load()),
		CompletedJobs:   int(q.completedJobsTally.Load()),
		RescheduledJobs: int(q.rescheduledJobsTally.Load()),
		ReleasedJobs:    int(q.releasedJobsTally.Load()),
	}
}

// Submit accepts one job and returns a handle that tracks its execution.
// ctx controls waiting for queue acceptance only. It does not cancel the running job.
func (q *Queue) Submit(ctx context.Context, job Job, opts ...SubmitOption) (*JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg := resolveSubmitConfig(opts)
	meta := q.newSubmissionMeta(cfg)
	handle := newJobHandle(meta)
	ok, err := q.acceptSubmission(job, submissionOptions{
		blocking:  !cfg.nonBlocking,
		acceptCtx: ctx,
		meta:      meta,
		handle:    handle,
	})
	if !ok {
		return nil, err
	}
	return handle, nil
}

// SubmitAfter accepts responsibility for submitting job after delay.
// The returned handle remains pending during the delay and tracks eventual execution.
// When the delay elapses, submission is non-blocking and may resolve with ErrQueueFull.
func (q *Queue) SubmitAfter(ctx context.Context, job Job, delay time.Duration, opts ...SubmitOption) (*JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	q.acceptMut.RLock()
	if q.IsStopped() {
		q.acceptMut.RUnlock()
		return nil, ErrQueueStopped
	}
	cfg := resolveSubmitConfig(opts)
	meta := q.newSubmissionMeta(cfg)
	handle := newJobHandle(meta)
	q.trackSubmission(handle)
	q.trackDelayed(handle, job)
	q.acceptMut.RUnlock()

	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()
		defer q.untrackDelayed(handle)
		select {
		case <-q.ctx.Done():
			if handle.rejectPending(ErrQueueStopped) {
				q.untrackSubmission(handle)
			}
		case <-handle.Done():
			q.untrackSubmission(handle)
		case <-timer.C:
			ok, err := q.acceptSubmission(job, submissionOptions{
				blocking:  false,
				acceptCtx: q.ctx,
				meta:      meta,
				handle:    handle,
			})
			if !ok && handle.rejectPending(err) {
				q.untrackSubmission(handle)
			}
		}
	}()
	return handle, nil
}

// SubmitAt accepts responsibility for submitting job at a specific time.
// The returned handle remains pending until that time and tracks eventual execution.
// When the time arrives, submission is non-blocking and may resolve with ErrQueueFull.
// If at is in the past, the job is submitted immediately.
func (q *Queue) SubmitAt(ctx context.Context, job Job, at time.Time, opts ...SubmitOption) (*JobHandle, error) {
	return q.SubmitAfter(ctx, job, time.Until(at), opts...)
}

// SubmitBatch submits jobs in order and returns handles for accepted jobs.
// If submission stops partway through, accepted handles and the rejection error are returned.
func (q *Queue) SubmitBatch(ctx context.Context, jobs []Job, opts ...SubmitOption) ([]*JobHandle, error) {
	handles := make([]*JobHandle, 0, len(jobs))
	for _, job := range jobs {
		handle, err := q.Submit(ctx, job, opts...)
		if err != nil {
			return handles, err
		}
		handles = append(handles, handle)
	}
	return handles, nil
}

// SubmitBatchAfter schedules jobs in order after delay.
// If scheduling stops partway through, accepted handles and the rejection error are returned.
func (q *Queue) SubmitBatchAfter(ctx context.Context, jobs []Job, delay time.Duration, opts ...SubmitOption) ([]*JobHandle, error) {
	handles := make([]*JobHandle, 0, len(jobs))
	for _, job := range jobs {
		handle, err := q.SubmitAfter(ctx, job, delay, opts...)
		if err != nil {
			return handles, err
		}
		handles = append(handles, handle)
	}
	return handles, nil
}

// newSubmissionMeta creates metadata for a newly accepted submission.
func (q *Queue) newSubmissionMeta(cfg submitConfig) JobMeta {
	id := cfg.id
	if id == "" {
		id = q.nextJobID()
	}
	return JobMeta{
		ID:         id,
		Name:       cfg.name,
		Attributes: cloneStringMap(cfg.attributes),
		EnqueuedAt: time.Now(),
	}
}

// reserveWorkerSlot checks limits under lock and reserves one running-worker slot.
// The tally is incremented before starting a goroutine to avoid oversubscribing workersMax.
func (q *Queue) reserveWorkerSlot() bool {
	q.mut.Lock()
	defer q.mut.Unlock()

	running := q.RunningWorkers()
	if running >= q.workersMax || (running >= q.workersMin && q.IdleWorkers() > 0) {
		return false
	}

	q.workersRunningTally.Add(1)
	return true
}

// reserveIdleWorkerStop reserves one idle worker to be stopped under lock.
// Tallies are decremented before signaling stop to avoid double-reserving capacity.
func (q *Queue) reserveIdleWorkerStop() bool {
	q.mut.Lock()
	defer q.mut.Unlock()

	if q.IdleWorkers() == 0 || q.RunningWorkers() <= q.workersMin {
		return false
	}

	q.workersRunningTally.Add(-1)
	q.workersIdleTally.Add(-1)
	return true
}

// resetWorkers resets running and idle worker tallies.
func (q *Queue) resetWorkers() {
	q.mut.Lock()
	defer q.mut.Unlock()

	q.workersRunningTally.Store(0)
	q.workersIdleTally.Store(0)
}

// closeJobs closes the jobs channel at most once.
func (q *Queue) closeJobs() {
	q.jobsCloseOnce.Do(func() {
		close(q.jobs)
	})
}

// markJobEnqueued records a job accepted by the queue.
func (q *Queue) markJobEnqueued() {
	q.createdJobsTally.Add(1)
	q.pendingJobsTally.Add(1)
}

// rollbackJobEnqueued reverts markJobEnqueued when enqueue fails.
func (q *Queue) rollbackJobEnqueued() {
	q.createdJobsTally.Add(-1)
	q.pendingJobsTally.Add(-1)
}

// markJobStarted records a pending job transitioning to active.
func (q *Queue) markJobStarted() {
	q.activeJobsTally.Add(1)
	q.pendingJobsTally.Add(-1)
}

// markJobFailed records a failed active job.
func (q *Queue) markJobFailed() {
	q.activeJobsTally.Add(-1)
	q.failedJobsTally.Add(1)
}

// markJobDiscarded records a discarded active job.
func (q *Queue) markJobDiscarded() {
	q.activeJobsTally.Add(-1)
	q.discardedJobsTally.Add(1)
}

// markJobCancelled records a cancelled active job.
func (q *Queue) markJobCancelled() {
	q.activeJobsTally.Add(-1)
	q.cancelledJobsTally.Add(1)
}

// markJobCompleted records a successfully completed active job.
func (q *Queue) markJobCompleted() {
	q.activeJobsTally.Add(-1)
	q.completedJobsTally.Add(1)
}

// markJobRescheduled records a reschedule request.
func (q *Queue) markJobRescheduled(reason string) {
	q.rescheduledJobsTally.Add(1)
	if reason == RescheduleReasonRelease || reason == RescheduleReasonReleaseSelf {
		q.releasedJobsTally.Add(1)
	}
}

// markWorkerIdle records a worker becoming idle.
func (q *Queue) markWorkerIdle() {
	q.workersIdleTally.Add(1)
}

// trackSubmission records a submission that has not reached a terminal state.
func (q *Queue) trackSubmission(handle *JobHandle) {
	q.submissionsMut.Lock()
	q.submissions[handle] = struct{}{}
	q.submissionsMut.Unlock()
}

// untrackSubmission removes a terminal submission from queue tracking.
func (q *Queue) untrackSubmission(handle *JobHandle) {
	q.submissionsMut.Lock()
	delete(q.submissions, handle)
	q.submissionsMut.Unlock()
}

// trackDelayed records a delayed submission awaiting its timer.
func (q *Queue) trackDelayed(handle *JobHandle, job Job) {
	q.submissionsMut.Lock()
	q.delayedJobs[handle] = job
	q.submissionsMut.Unlock()
}

// untrackDelayed removes a delayed submission once its timer resolves.
func (q *Queue) untrackDelayed(handle *JobHandle) {
	q.submissionsMut.Lock()
	delete(q.delayedJobs, handle)
	q.submissionsMut.Unlock()
}

// abandonPendingSubmissions completes every tracked pending submission as abandoned.
func (q *Queue) abandonPendingSubmissions() {
	q.submissionsMut.Lock()
	defer q.submissionsMut.Unlock()
	for handle := range q.submissions {
		handle.abandon()
		delete(q.submissions, handle)
	}
}

// unmarkWorkerIdle records a worker leaving idle state.
func (q *Queue) unmarkWorkerIdle() {
	q.workersIdleTally.Add(-1)
}

// nextJobID generates a unique job ID.
// If an ID generator is configured, it is used to generate a unique ID.
// Otherwise, a counter is used to generate a unique ID.
func (q *Queue) nextJobID() string {
	if q.idGenerator != nil {
		if id := q.idGenerator(); id != "" {
			return id
		}
	}
	return strconv.FormatInt(q.jobIDCounter.Add(1), 10)
}

// acceptSubmission accepts a submission into the queue internals.
// It first tries to start a dedicated worker for the job when scaling limits allow.
// If no worker can be started, it falls back to pushing the job onto `q.jobs`.
// When `blocking` is false, the fallback channel send is non-blocking.
func (q *Queue) acceptSubmission(job Job, opts submissionOptions) (ok bool, err error) {
	if job == nil {
		return false, ErrQueueJobRequired
	}
	if opts.acceptCtx != nil {
		select {
		case <-opts.acceptCtx.Done():
			return false, opts.acceptCtx.Err()
		default:
		}
	}
	if opts.handle != nil && opts.handle.state.Load() != submissionPending {
		return false, opts.handle.terminalError()
	}

	q.acceptMut.RLock()
	if q.IsStopped() {
		q.acceptMut.RUnlock()
		return false, ErrQueueStopped
	}
	if q.IsPaused() && q.pauseBehavior == PauseReject {
		q.acceptMut.RUnlock()
		return false, ErrQueuePaused
	}

	// Create job metadata.
	if opts.handle == nil {
		opts.handle = newJobHandle(opts.meta)
	}
	if opts.meta.ID == "" {
		// Generate a new job ID.
		opts.meta.ID = q.nextJobID()
	}
	opts.meta.EnqueuedAt = time.Now()
	opts.handle.setMeta(opts.meta)
	meta := opts.meta

	// Apply queue-level middleware, keeping the as-submitted job for drain handback.
	rawJob := job
	job = q.applyMiddleware(job)

	// Wrap the job with a job metadata context.
	// Additionally, dispatch the start and result of the job.
	wrappedJob := func(ctx context.Context) error {
		executionCtx, cancel := context.WithCancelCause(ctx)
		// Immediate shutdown may abandon a buffered submission before a worker starts it.
		if !opts.handle.start(time.Now(), cancel) {
			cancel(nil)
			q.untrackSubmission(opts.handle)
			if err := opts.handle.terminalError(); err != nil {
				return err
			}
			return ErrJobAbandoned
		}
		defer cancel(nil)
		defer q.untrackSubmission(opts.handle)

		// Create a new context with the job metadata.
		jobCtx := contextWithMetaOwned(executionCtx, meta)

		// Create a new context with the retry attempt emitter only when needed.
		if q.hasAttemptHooks {
			jobCtx = contextWithRetryAttemptEmitter(jobCtx, retryAttemptEmitter{
				start: func(hookCtx context.Context, hookMeta JobMeta, startedAt time.Time) {
					q.dispatchAttemptStart(hookCtx, hookMeta, startedAt)
				},
				result: func(hookCtx context.Context, hookMeta JobMeta, hookErr error, startedAt, finishedAt time.Time) {
					q.dispatchAttemptResult(hookCtx, hookMeta, hookErr, startedAt, finishedAt)
				},
			})
		}

		// Create a new context with the discard marker.
		discarded := false
		jobCtx = contextWithDiscardMarker(jobCtx, func() {
			discarded = true
		})

		// Dispatch the start hook event.
		startedAt := time.Now()
		q.dispatchStart(jobCtx, meta, startedAt)

		// Convert a job or middleware panic into the submission's terminal result.
		var err error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					err = &PanicError{Value: recovered, Origin: PanicOriginJob}
					if q.panicHandler != nil {
						q.panicHandler(err)
					}
				}
			}()
			err = job(jobCtx)
		}()
		if errors.Is(context.Cause(executionCtx), ErrJobCancelled) && errors.Is(err, context.Canceled) {
			err = ErrJobCancelled
		}

		// Dispatch the result hook event.
		finishedAt := time.Now()
		if discarded && err == nil {
			err = errQueueDiscardedOutcome
			_ = opts.handle.finish(finishedAt, nil)
		} else {
			err = opts.handle.finish(finishedAt, err)
		}
		q.dispatchResult(jobCtx, meta, err, startedAt, finishedAt)

		return err
	}

	// Buffered item carrying the handle and as-submitted job for drain handback.
	item := &queuedJob{run: wrappedJob, raw: rawJob, handle: opts.handle}

	// Track the job and record acceptance tallies.
	q.jobWg.Add(1)
	q.markJobEnqueued()
	q.trackSubmission(opts.handle)
	q.acceptMut.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			// Panic occurred. Notify handler if configured.
			if q.panicHandler != nil {
				q.panicHandler(r)
			}
			if err == nil {
				err = fmt.Errorf("queue: submission acceptance panic: %v", r)
			}
		}
		if !ok {
			// Job could not be accepted, rollback tallies.
			q.rollbackJobEnqueued()
			q.jobWg.Done()
			q.untrackSubmission(opts.handle)
		}
	}()

	// Attempt to create a dedicated worker for this job when not paused.
	// If paused, we still accept enqueue and place work into q.jobs.
	if !q.IsPaused() {
		ok = q.newWorker(wrappedJob)
	}
	if !ok {
		// Either no worker slot was available or queue is paused.
		// Fallback is to buffer the job in q.jobs.
		if opts.blocking {
			if opts.acceptCtx == nil {
				// No acceptance context provided, so block until the job is accepted.
				q.jobs <- item
				ok = true
			} else {
				// Acceptance context provided, so block until the job is accepted or the context is done.
				select {
				case q.jobs <- item:
					ok = true
				case <-opts.acceptCtx.Done():
					ok = false
					err = opts.acceptCtx.Err()
				}
			}
		} else {
			select {
			case q.jobs <- item:
				ok = true
			default:
				ok = false
				err = ErrQueueFull
			}
		}
	}
	if ok {
		hookCtx := opts.acceptCtx
		if hookCtx == nil {
			hookCtx = q.ctx
		}
		q.dispatchEnqueue(hookCtx, meta)
		err = nil
	}
	return ok, err
}

// applyMiddleware applies queue-level middleware in registration order (first to last).
// WithMiddleware(a, b) executes as a(b(job)).
func (q *Queue) applyMiddleware(job Job) Job {
	if len(q.middleware) == 0 {
		return job
	}

	wrapped := job
	for i := len(q.middleware) - 1; i >= 0; i-- {
		mw := q.middleware[i]
		if mw == nil {
			continue
		}
		wrapped = mw(wrapped)
	}
	return wrapped
}

// workJob executes a single job.
// `isFirst` is true when the job is the worker's initial dedicated job.
func (q *Queue) workJob(job Job, isFirst bool) {
	if ok := q.waitWhilePaused(); !ok {
		return
	}

	// Preserve queue accounting if worker or wrapped-job infrastructure panics.
	// User job and middleware panics are normally converted to errors by wrappedJob.
	defer func() {
		if r := recover(); r != nil {
			err := &PanicError{Value: r, Origin: PanicOriginWorker}

			// Update job tallies and run panic handler if set.
			q.markJobFailed()
			if q.panicHandler != nil {
				q.panicHandler(err)
			} else {
				// Default behavior: log to stderr if no handler is set.
				fmt.Fprintf(os.Stderr, "workJob: job panic (no handler set): %v\n", err)
			}
		}

		// Mark the job done and return the worker to idle state.
		q.jobWg.Done()
		q.markWorkerIdle()
	}()

	if !isFirst {
		// Worker is taking a queued job, so it is no longer idle.
		q.unmarkWorkerIdle()
	}

	// Update tallies.
	q.markJobStarted()

	// Run the job with the queue context and record the result.
	if err := job(q.ctx); errors.Is(err, ErrJobCancelled) {
		q.markJobCancelled()
	} else if errors.Is(err, ErrDiscard) || errors.Is(err, errQueueDiscardedOutcome) {
		q.markJobDiscarded()
	} else if err != nil {
		q.markJobFailed()
	} else {
		q.markJobCompleted()
	}
}

// workJobs runs the worker loop until context cancellation or idle-stop signal.
// If `initialJob` is set, it is executed before the loop starts.
func (q *Queue) workJobs(initialJob Job) {
	defer q.workerWg.Done()

	if initialJob != nil {
		if ok := q.waitWhilePaused(); !ok {
			return
		}

		// Execute the initial job provided.
		q.workJob(initialJob, true)
	}

	for {
		if ok := q.waitWhilePaused(); !ok {
			return
		}

		select {
		case <-q.ctx.Done():
			return // Context cancelled, stop worker.
		case item := <-q.jobs:
			if item == nil {
				return // Received a nil item, so this worker must be idle: exit.
			}
			q.workJob(item.run, false)
		}
	}
}

// newWorker tries to start a worker when scaling limits allow.
// If `initialJob` is set, the worker executes it immediately.
// Returns false when no worker slot can be reserved.
func (q *Queue) newWorker(initialJob Job) (ok bool) {
	if ok = q.reserveWorkerSlot(); !ok {
		return // No worker slot available.
	}

	if initialJob == nil {
		// Worker is idle until it takes a job.
		q.markWorkerIdle()
	}

	// Add to worker wait group and start the worker loop.
	q.workerWg.Add(1)
	go q.workJobs(initialJob)
	ok = true
	return
}

// pollDistributedPause periodically syncs pause state from the configured store.
func (q *Queue) pollDistributedPause() {
	tick := q.pausePollTick
	if tick <= 0 {
		tick = defaultPausePollTick
	}

	t := time.NewTicker(tick)
	defer t.Stop()
	defer q.workerWg.Done()

	// Sync immediately on startup.
	q.syncDistributedPause()

	for {
		select {
		case <-q.ctx.Done():
			return
		case <-t.C:
			q.syncDistributedPause()
		}
	}
}

// syncDistributedPause fetches pause state from the store and updates local cache.
func (q *Queue) syncDistributedPause() {
	if q.pauseStore == nil || q.pauseStoreKey == "" {
		return // No distributed pause store, skip sync.
	}

	paused, err := q.pauseStore.IsPaused(q.ctx, q.pauseStoreKey)
	if err != nil {
		if q.panicHandler != nil {
			q.panicHandler(fmt.Errorf("queue: pause poll: %w", err))
		}
		return
	}

	q.distPaused.Store(paused)
}

// waitWhilePaused blocks worker execution while the queue is paused.
// It returns false when queue context is cancelled.
func (q *Queue) waitWhilePaused() bool {
	if !q.IsPaused() {
		return true
	}

	t := time.NewTicker(defaultPauseWaitTick)
	defer t.Stop()

	for q.IsPaused() {
		select {
		case <-q.ctx.Done():
			return false
		case <-t.C:
		}
	}

	return true
}

// cleanupIdleWorkers periodically tries to stop extra idle workers.
// It ticks at `workerIdleTick` and signals a worker to exit by sending nil.
func (q *Queue) cleanupIdleWorkers() {
	tick := q.workerIdleTick
	if tick <= 0 {
		tick = defaultWorkerIdleTick
	}

	pt := time.NewTicker(tick)
	defer pt.Stop()
	defer q.workerWg.Done()

	for {
		select {
		case <-q.ctx.Done():
			return // Context cancelled, stop idle worker ticker.
		case <-pt.C:
			if ok := q.reserveIdleWorkerStop(); ok {
				q.jobs <- nil // Send nil to signal an idle worker to exit.
			}
		}
	}
}

// Functional options for queue.
type QueueOption func(*Queue)

// WithWorkerIdleTick sets how often idle-worker cleanup runs.
func WithWorkerIdleTick(tt time.Duration) QueueOption {
	return func(q *Queue) {
		if tt <= 0 {
			q.workerIdleTick = defaultWorkerIdleTick
			return
		}
		q.workerIdleTick = tt
	}
}

// WithContext sets the queue context and derives its cancel function.
func WithContext(ctx context.Context) QueueOption {
	return func(q *Queue) {
		q.ctx, q.ctxCancel = context.WithCancel(ctx)
	}
}

// WithCancelableContext sets the queue context and cancel function directly.
// This is useful with signal-aware contexts to stop on SIGTERM/SIGINT.
func WithCancelableContext(ctx context.Context, ctxCancel context.CancelFunc) QueueOption {
	return func(q *Queue) {
		q.ctx, q.ctxCancel = ctx, ctxCancel
	}
}

// WithPanicHandler sets a panic handler for job and queue panics.
// The handler receives the value returned by recover.
func WithPanicHandler(handler func(any)) QueueOption {
	return func(q *Queue) {
		q.panicHandler = handler
	}
}

// WithIDGenerator sets a custom generator for submission job IDs.
// Returning an empty ID falls back to the default atomic counter.
func WithIDGenerator(gen IDGenerator) QueueOption {
	return func(q *Queue) {
		q.idGenerator = gen
	}
}

// WithQueueName sets a stable queue name for observability payloads.
func WithQueueName(name string) QueueOption {
	return func(q *Queue) {
		q.name = name
	}
}

// WithPauseStore enables distributed pause synchronization.
// `key` identifies this queue in the store.
func WithPauseStore(store PauseStore, key string) QueueOption {
	return func(q *Queue) {
		q.pauseStore = store
		q.pauseStoreKey = key
	}
}

// WithPausePollTick sets how often distributed pause state is refreshed.
func WithPausePollTick(tt time.Duration) QueueOption {
	return func(q *Queue) {
		if tt <= 0 {
			q.pausePollTick = defaultPausePollTick
			return
		}
		q.pausePollTick = tt
	}
}

// WithPauseBehavior sets enqueue behavior while queue execution is paused.
// Defaults to PauseBuffer.
func WithPauseBehavior(pb PauseBehavior) QueueOption {
	return func(q *Queue) {
		q.pauseBehavior = pb
	}
}

// WithMiddleware registers queue-level middleware wrappers.
// Middleware is applied in registration order.
func WithMiddleware(mw ...Middleware) QueueOption {
	return func(q *Queue) {
		q.middleware = append(q.middleware, mw...)
	}
}

// WithHooks sets optional lifecycle hooks for queue transitions.
func WithHooks(hooks Hooks) QueueOption {
	return func(q *Queue) {
		q.hooks = append(q.hooks, hooks)
		if hooks.OnAttemptStart != nil || hooks.OnAttemptSuccess != nil || hooks.OnAttemptFailure != nil {
			q.hasAttemptHooks = true
		}
	}
}

// resolveSubmitConfig applies submission options.
func resolveSubmitConfig(opts []SubmitOption) submitConfig {
	cfg := submitConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

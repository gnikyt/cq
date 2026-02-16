package cq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const defaultWorkerIdleTick = 5 * time.Second // Every five seconds check for idle workers.

// Queue dispatches jobs to workers.
// It dynamically scales workers between configured minimum and maximum limits,
// and tracks runtime job and worker metrics.
type Queue struct {
	workersMin          int                // Minimum worker count.
	workersMax          int                // Maximum worker count.
	workerIdleTick      time.Duration      // Interval for idle-worker cleanup.
	workerWg            sync.WaitGroup     // Tracks worker and cleanup goroutines.
	jobWg               sync.WaitGroup     // Tracks in-flight jobs.
	jobs                chan Job           // Buffered job queue.
	ctx                 context.Context    // Queue context for workers/jobs.
	ctxCancel           context.CancelFunc // Cancels the queue context.
	panicHandler        func(any)          // Optional panic handler for job panics.
	envelopeStore       EnvelopeStore      // Optional persistence hook for enqueue/ack/nack.
	mut                 sync.Mutex         // Guards worker scaling decisions.
	enqueueMut          sync.RWMutex       // Synchronizes enqueue tracking with Stop/Terminate.
	stopped             atomic.Bool        // Indicates queue shutdown has started.
	workersRunningTally atomic.Int32       // Reserved/active worker slots used for scaling decisions.
	workersIdleTally    atomic.Int32       // Reserved idle worker slots available for new jobs.
	createdJobsTally    atomic.Int64       // Total jobs accepted.
	activeJobsTally     atomic.Int64       // Jobs currently executing.
	pendingJobsTally    atomic.Int64       // Jobs waiting in the queue.
	failedJobsTally     atomic.Int64       // Jobs completed with error.
	completedJobsTally  atomic.Int64       // Jobs completed successfully.
	jobIDCounter        atomic.Int64       // Counter for generating unique job IDs.
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
		jobs:           make(chan Job, cap),
		jobWg:          sync.WaitGroup{},
		workerWg:       sync.WaitGroup{},
		workerIdleTick: defaultWorkerIdleTick,
	}
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
	return q.workersMin, q.workersMax
}

// Capacity returns the capacity of the jobs channel.
func (q *Queue) Capacity() int {
	return cap(q.jobs)
}

// Start begins the idle-worker ticker and starts the configured minimum workers.
func (q *Queue) Start() {
	q.workerWg.Add(1)
	go q.cleanupIdleWorkers()

	for range q.workersMin {
		q.newWorker(nil)
	}
}

// Stop gracefully shuts down the queue.
// It marks the queue as stopped, optionally waits for queued jobs to finish
// when `jobWait` is true, waits for worker goroutines to exit, resets worker
// tallies, and closes the jobs channel.
func (q *Queue) Stop(jobWait bool) {
	q.enqueueMut.Lock()
	defer q.enqueueMut.Unlock()

	q.stopped.Store(true)
	if jobWait {
		q.jobWg.Wait()
	}
	q.ctxCancel()
	q.workerWg.Wait()
	q.resetWorkers()
	close(q.jobs)
}

// Terminate forces an immediate shutdown.
// Unlike Stop, it does not wait for jobs or worker goroutines to finish.
func (q *Queue) Terminate() {
	q.enqueueMut.Lock()
	defer q.enqueueMut.Unlock()

	q.stopped.Store(true)
	q.ctxCancel()
	q.resetWorkers()
	close(q.jobs)
}

// IsStopped atomically checks if the queue is stopped.
func (q *Queue) IsStopped() bool {
	return q.stopped.Load()
}

// TallyOf atomically returns the number of jobs for a given state.
func (q *Queue) TallyOf(js JobState) int {
	var val int64
	switch js {
	case JobStateCompleted:
		val = q.completedJobsTally.Load()
	case JobStateFailed:
		val = q.failedJobsTally.Load()
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

// Enqueue submits a job to the queue.
// It blocks if no worker can be started and the jobs channel is full.
func (q *Queue) Enqueue(job Job) {
	q.doEnqueue(job, true)
}

// TryEnqueue attempts to submit a job without blocking.
// It returns true if accepted, or false if no worker can be started and the
// jobs channel is full.
func (q *Queue) TryEnqueue(job Job) bool {
	return q.doEnqueue(job, false)
}

// DelayEnqueue submits a job after the given delay.
// The delay runs in its own goroutine so the caller is not blocked.
func (q *Queue) DelayEnqueue(job Job, delay time.Duration) {
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()

		select {
		case <-q.ctx.Done():
			return // Context cancelled, stop delay.
		case <-timer.C:
			q.doEnqueue(job, true) // Job can be submitted now.
		}
	}()
}

// EnqueueBatch accepts a slice of jobs and enqueues each one.
func (q *Queue) EnqueueBatch(jobs []Job) {
	for _, job := range jobs {
		q.doEnqueue(job, true)
	}
}

// DelayEnqueueBatch submits a slice of jobs after the given delay.
func (q *Queue) DelayEnqueueBatch(jobs []Job, delay time.Duration) {
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()

		select {
		case <-q.ctx.Done():
			return // Context cancelled, stop delay.
		case <-timer.C:
			for _, job := range jobs {
				q.doEnqueue(job, true) // Jobs can be submitted now.
			}
		}
	}()
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

// markJobCompleted records a successfully completed active job.
func (q *Queue) markJobCompleted() {
	q.activeJobsTally.Add(-1)
	q.completedJobsTally.Add(1)
}

// markWorkerIdle records a worker becoming idle.
func (q *Queue) markWorkerIdle() {
	q.workersIdleTally.Add(1)
}

// unmarkWorkerIdle records a worker leaving idle state.
func (q *Queue) unmarkWorkerIdle() {
	q.workersIdleTally.Add(-1)
}

// nextJobID generates a unique job ID.
func (q *Queue) nextJobID() string {
	return strconv.FormatInt(q.jobIDCounter.Add(1), 10)
}

// reportEnvelopeEnqueue reports an accepted enqueue to persistence, if configured.
func (q *Queue) reportEnvelopeEnqueue(meta JobMeta) {
	if q.envelopeStore == nil {
		return
	}

	if err := q.envelopeStore.Enqueue(q.ctx, Envelope{
		ID:          meta.ID,
		Attempt:     meta.Attempt,
		Status:      EnvelopeStatusEnqueued,
		EnqueuedAt:  meta.EnqueuedAt,
		AvailableAt: time.Now(),
	}); err != nil && q.panicHandler != nil {
		q.panicHandler(fmt.Errorf("queue: envelope enqueue: %w", err))
	}
}

// reportEnvelopeResult reports final result to persistence, if configured.
func (q *Queue) reportEnvelopeResult(meta JobMeta, err error) {
	if q.envelopeStore == nil {
		return
	}

	var repErr error
	if err != nil {
		repErr = q.envelopeStore.Nack(q.ctx, meta.ID, err)
	} else {
		repErr = q.envelopeStore.Ack(q.ctx, meta.ID)
	}

	if repErr != nil && q.panicHandler != nil {
		q.panicHandler(fmt.Errorf("queue: envelope result: %w", repErr))
	}
}

// reportEnvelopeClaim reports "claimed/started" to persistence.
func (q *Queue) reportEnvelopeClaim(meta JobMeta) {
	if q.envelopeStore == nil {
		return
	}

	if err := q.envelopeStore.Claim(q.ctx, meta.ID); err != nil && q.panicHandler != nil {
		q.panicHandler(fmt.Errorf("queue: envelope claim: %w", err))
	}
}

// reportEnvelopeReschedule reports deferred execution for an envelope.
func (q *Queue) reportEnvelopeReschedule(meta JobMeta, delay time.Duration, reason string) {
	if q.envelopeStore == nil || meta.ID == "" {
		return
	}

	nextRunAt := time.Now().Add(delay)
	if err := q.envelopeStore.Reschedule(q.ctx, meta.ID, nextRunAt, reason); err != nil && q.panicHandler != nil {
		q.panicHandler(fmt.Errorf("queue: envelope reschedule: %w", err))
	}
}

// doEnqueue submits a job to the queue internals.
// It first tries to start a dedicated worker for the job when scaling limits allow.
// If no worker can be started, it falls back to pushing the job onto `q.jobs`.
// When `blocking` is false, the fallback channel send is non-blocking.
func (q *Queue) doEnqueue(job Job, blocking bool) (ok bool) {
	q.enqueueMut.RLock()
	if q.IsStopped() {
		q.enqueueMut.RUnlock()
		return // Queue stopped.
	}

	// Create job metadata.
	meta := JobMeta{
		ID:         q.nextJobID(),
		EnqueuedAt: time.Now(),
		Attempt:    0,
	}
	wrappedJob := func(ctx context.Context) error {
		attemptCtx := contextWithMeta(ctx, meta)
		q.reportEnvelopeClaim(meta)
		err := job(attemptCtx)
		q.reportEnvelopeResult(meta, err)
		return err
	}

	// Track the job and record enqueue tallies.
	q.jobWg.Add(1)
	q.markJobEnqueued()
	q.enqueueMut.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			// Panic occurred; notify handler if configured.
			if q.panicHandler != nil {
				q.panicHandler(r)
			}
		}
		if !ok {
			// Job could not be enqueued, rollback tallies.
			q.rollbackJobEnqueued()
			q.jobWg.Done()
		}
	}()

	// Attempt to create a dedicated worker for this job.
	if ok = q.newWorker(wrappedJob); !ok {
		if blocking {
			q.jobs <- wrappedJob
			ok = true
		} else {
			select {
			case q.jobs <- wrappedJob:
				ok = true
			default:
				ok = false
			}
		}
	}
	if ok {
		q.reportEnvelopeEnqueue(meta)
	}
	return
}

// workJob executes a single job.
// `isFirst` is true when the job is the worker's initial dedicated job.
func (q *Queue) workJob(job Job, isFirst bool) {
	// No matter what, mark job as done and attempt to
	// recover from a panic in the handler.
	defer func() {
		if r := recover(); r != nil {
			var err error
			switch x := r.(type) {
			case string:
				err = fmt.Errorf("queue: work job panic: %s", x)
			case error:
				err = fmt.Errorf("queue: work job panic: %w", x)
			default:
				err = fmt.Errorf("queue: work job panic: %v", x)
			}

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
	if err := job(q.ctx); err != nil {
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
		// Execute the initial job provided.
		q.workJob(initialJob, true)
	}

	for {
		select {
		case <-q.ctx.Done():
			return // Context cancelled, stop worker.
		case job := <-q.jobs:
			if job == nil {
				return // Received a nil job, so this worker must be idle: exit.
			}
			q.workJob(job, false)
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

// cleanupIdleWorkers periodically tries to stop extra idle workers.
// It ticks at `workerIdleTick` and signals a worker to exit by sending nil.
func (q *Queue) cleanupIdleWorkers() {
	pt := time.NewTicker(q.workerIdleTick)
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

// WithEnvelopeStore sets an optional persistence adapter for enqueue/ack/nack reporting.
func WithEnvelopeStore(store EnvelopeStore) QueueOption {
	return func(q *Queue) {
		q.envelopeStore = store
	}
}

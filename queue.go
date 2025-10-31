package cq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultWorkerIdleTick = 5 * time.Second // Every five seconds check for idle workers.
)

// Queue is responsible for pushing jobs to workers.
// It will dynamically scale workers, if parameters for min
// and max allow it to do so. As well, it keeps track of
// various job metrics.
type Queue struct {
	workersMin          int                // Minimum number of workers.
	workersMax          int                // Maximum number of workers.
	workerIdleTick      time.Duration      // Idle worker timeout.
	workerWg            sync.WaitGroup     // Wait group for workers.
	jobWg               sync.WaitGroup     // Wait group of jobs.
	jobs                chan Job           // Job channel.
	ctx                 context.Context    // Context.
	ctxCancel           context.CancelFunc // Context cancel function.
	panicHandler        func(any)          // Panic handler.
	mut                 sync.Mutex         // Mutex for handling checks on active worker space.
	stopped             atomic.Bool        // Done flag.
	workersRunningTally atomic.Int32       // Number of running workers. int32 should suffice, allows for decrements.
	workersIdleTally    atomic.Int32       // Number of idle workers. int32 should suffice, allows for decrements.
	createdJobsTally    atomic.Int64       // Number of jobs pushed.
	activeJobsTally     atomic.Int64       // Number of active jobs.
	pendingJobsTally    atomic.Int64       // Number of pending jobs to be processed.
	failedJobsTally     atomic.Int64       // Number of jobs failed.
	completedJobsTally  atomic.Int64       // Number of completed jobs.
}

// NewQueue returns a new Queue.
// Pass in the minumum running workers as wmin, pass in the maxiumum
// workers as wmax, and a job capacity as cap. Additional configuration
// options can be passed in as the remaining parameters.
func NewQueue(wmin int, wmax int, cap int, opts ...QueueOption) *Queue {
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

// Start will begin the idle worker ticker and start the minimum
// number of workers configured.
func (q *Queue) Start() {
	q.workerWg.Add(1)
	go q.cleanupIdleWorkers()
	for i := 0; i < q.workersMin; i += 1 {
		q.newWorker(nil)
	}
}

// Stop atomically sets the flag to tell the workers to stop when done
// processing the jobs and additionally resets the tallies and closes
// the jobs channel. If jobWait is true, in addition to waiting for the
// workers to finish, it will also wait for the jobs in the queue to
// finish.
func (q *Queue) Stop(jobWait bool) {
	(&q.stopped).Store(true)
	if jobWait {
		q.jobWg.Wait()
	}
	q.ctxCancel()
	q.workerWg.Wait()
	q.resetWorkers()
	close(q.jobs)
}

// Terminate is the same as Stop except it will immediately stop
// everything without waits on jobs or workers.
func (q *Queue) Terminate() {
	(&q.stopped).Store(true)
	q.ctxCancel()
	q.resetWorkers()
	close(q.jobs)
}

// IsStopped atomically checks if the queue is stopped.
func (q *Queue) IsStopped() bool {
	return (&q.stopped).Load()
}

// TallyOf atomically returns the number of jobs for a given state.
func (q *Queue) TallyOf(js JobState) int {
	var val int64
	switch js {
	case JobStateCompleted:
		val = (&q.completedJobsTally).Load()
	case JobStateFailed:
		val = (&q.failedJobsTally).Load()
	case JobStatePending:
		val = (&q.pendingJobsTally).Load()
	case JobStateActive:
		val = (&q.activeJobsTally).Load()
	default:
		val = (&q.createdJobsTally).Load()
	}
	return int(val)
}

// RunningWorkers atomically returns the number of running workers.
func (q *Queue) RunningWorkers() int {
	return int((&q.workersRunningTally).Load())
}

// IdleWorkers atomically returns the number of idle workers.
func (q *Queue) IdleWorkers() int {
	return int((&q.workersIdleTally).Load())
}

// addRunningWorker checks to see if active workers can be incremented.
func (q *Queue) addRunningWorker() bool {
	q.mut.Lock()
	defer q.mut.Unlock()

	active := q.RunningWorkers()
	if active >= q.workersMax || (active >= q.workersMin && q.IdleWorkers() > 0) {
		return false
	}
	(&q.workersRunningTally).Add(1)
	return true
}

// subtrackRunningWorker checks to see if running workers can be decremented.
func (q *Queue) subtractRunningWorker() bool {
	q.mut.Lock()
	defer q.mut.Unlock()

	if q.IdleWorkers() == 0 || q.RunningWorkers() <= q.workersMin {
		return false
	}
	(&q.workersRunningTally).Add(-1)
	(&q.workersIdleTally).Add(-1)
	return true
}

// resetWorkers will reset the running and idle values.
func (q *Queue) resetWorkers() {
	q.mut.Lock()
	defer q.mut.Unlock()
	(&q.workersRunningTally).Store(0)
	(&q.workersIdleTally).Store(0)
}

// Enqueue accepts a job and pushes it into the queue. Non-blocking.
// If queue is at capacity, this will cause a panic.
func (q *Queue) Enqueue(job Job) {
	q.doEnqueue(job)
}

// TryEnqueue accepts a job and trys to push it into the queue. Blocking.
// Will return true if was added, false if not (queue capacity reached).
func (q *Queue) TryEnqueue(job Job) bool {
	return q.doEnqueue(job)
}

// DelayEnqueue accepts a job and will push it into the queue after
// the set delay duration. It will do this its own goroutine to not
// have to block other actions of your code.
func (q *Queue) DelayEnqueue(job Job, delay time.Duration) {
	go func() {
		<-time.After(delay)
		q.doEnqueue(job)
	}()
}

// EnqueueBatch accepts a slice of jobs and enqueues each one.
func (q *Queue) EnqueueBatch(jobs []Job) {
	for _, job := range jobs {
		q.doEnqueue(job)
	}
}

// DelayEnqueueBatch accepts a slice of jobs and will push them into
// the queue after the set delay duration.
func (q *Queue) DelayEnqueueBatch(jobs []Job, delay time.Duration) {
	go func() {
		<-time.After(delay)
		for _, job := range jobs {
			q.doEnqueue(job)
		}
	}()
}

// doEnqueue accepts a job and attempts to start a worker initially dedicated
// to this job, so long as we are not above our maximum number of workers.
// If it fails to start a dedicated worker, it will push the job into the
// queue for an idling worker to pick up on when freed.
func (q *Queue) doEnqueue(job Job) (ok bool) {
	if q.IsStopped() {
		// Do not submit, queue is stopped. Panic could happen if submitted.
		return
	}

	// Add job to job waitgroup, add to tallies.
	q.jobWg.Add(1)
	(&q.createdJobsTally).Add(1)
	(&q.pendingJobsTally).Add(1)

	defer func() {
		if r := recover(); r != nil || !ok {
			// Try and run panic handler.
			if q.panicHandler != nil {
				q.panicHandler(r)
			}
			// Was not able to add to queue, everything is full.
			(&q.createdJobsTally).Add(-1)
			(&q.pendingJobsTally).Add(-1)
			q.jobWg.Done()
		}
	}()

	// Attempt to create a dedicated worker for this job.
	if ok = q.newWorker(job); !ok {
		q.jobs <- job
		ok = true
	}
	return
}

// workJob handles processing a job from queue.
// If isFirst is true, we know that the job passed in
// is being processed as dedicated.
func (q *Queue) workJob(job Job, isFirst bool) {
	// No matter what, mark job as done and attempt to
	// recover from a panic in the handler.
	defer func() {
		if r := recover(); r != nil {
			var err error
			switch x := r.(type) {
			case string:
				err = fmt.Errorf("workJob: %s", x)
			case error:
				err = fmt.Errorf("workJob: %w", x)
			default:
				err = fmt.Errorf("workJob: unknown panic: %v", x)
			}

			// Update tallies, run panic handler if set.
			(&q.activeJobsTally).Add(-1)
			(&q.failedJobsTally).Add(1)
			if q.panicHandler != nil {
				q.panicHandler(err)
			} else {
				// Default behavior: log to stderr.
				fmt.Fprintf(os.Stderr, "workJob: job panic (no handler set): %v\n", err)
			}
		}

		// Mark job done, mark worker as idle as it is not processing a job anymore.
		q.jobWg.Done()
		(&q.workersIdleTally).Add(1)
	}()

	if !isFirst {
		// Not the first job this worker has ran, mark as not idle.
		(&q.workersIdleTally).Add(-1)
	}

	// Update tallies.
	(&q.activeJobsTally).Add(1)
	(&q.pendingJobsTally).Add(-1)

	// Run the job with the queue's context, then update the tallies.
	if err := job(q.ctx); err != nil {
		(&q.activeJobsTally).Add(-1)
		(&q.failedJobsTally).Add(1)
	} else {
		(&q.activeJobsTally).Add(-1)
		(&q.completedJobsTally).Add(1)
	}
}

// workJobs handles processing the channels for jobs, failures, done, etc.
// If initial job is provided, this job will be worked on right away.
func (q *Queue) workJobs(initialJob Job) {
	defer q.workerWg.Done()
	if initialJob != nil {
		// Initial job provided, work it right away.
		q.workJob(initialJob, true)
	}
	for {
		select {
		case <-q.ctx.Done():
			// Context closed, stop worker.
			return
		case job := <-q.jobs:
			if job == nil {
				// Recieved a nil job, so this worker must be idle: exit.
				return
			}
			q.workJob(job, false)
		}
	}
}

// newWorker will attempt to create a new worker, if allowed by limits.
// If an initial job is provided, this will be worked on right away by
// the new spun-up worker.
// If a new worker can not be created, the job will not be processed
// unless pushed into the job channel somewhere else.
func (q *Queue) newWorker(initialJob Job) (ok bool) {
	if ok = q.addRunningWorker(); !ok {
		// Can not add a worker.
		return
	}
	if initialJob == nil {
		// No initial job provided, mark this worker idle.
		(&q.workersIdleTally).Add(1)
	}
	// Add to worker wait group, go listen for jobs.
	q.workerWg.Add(1)
	go q.workJobs(initialJob)
	ok = true
	return
}

// cleanupIdleWorkers will take the workerIdleTick and
// run a ticker at that interval to try and stop idle
// workers. Idle workers are stopped by sending a nil
// job to the job channel.
func (q *Queue) cleanupIdleWorkers() {
	pt := time.NewTicker(q.workerIdleTick)
	defer pt.Stop()
	defer q.workerWg.Done()
	for {
		select {
		case <-q.ctx.Done():
			// Context closed, stop idle worker ticker.
			return
		case <-pt.C:
			if ok := q.subtractRunningWorker(); ok {
				// An idle worker can be stopped.
				q.jobs <- nil
			}
		}
	}
}

// Functional options for queue.
type QueueOption func(*Queue)

// WithWorkerIdleTick is a functional option for Queue to
// set when idle workers timeout for cleanup.
func WithWorkerIdleTick(tt time.Duration) QueueOption {
	return func(q *Queue) {
		q.workerIdleTick = tt
	}
}

// WithContext is a functional option for Queue to
// set a context handler for the queue itself.
func WithContext(ctx context.Context) QueueOption {
	return func(q *Queue) {
		q.ctx, q.ctxCancel = context.WithCancel(ctx)
	}
}

// WithCancelableContext is a functional option for Queue to
// set a context handler for the queue itself.
// This is useful for passing in a signal notify context
// for example, to tell the queue to stop on sigterm/sigint.
func WithCancelableContext(ctx context.Context, ctxCancel context.CancelFunc) QueueOption {
	return func(q *Queue) {
		q.ctx, q.ctxCancel = ctx, ctxCancel
	}
}

// WithPanicHandler allows for manually handling panics for jobs
// or for the queue itself.
// The handler is passed in the value from recover.
func WithPanicHandler(handler func(any)) QueueOption {
	return func(q *Queue) {
		q.panicHandler = handler
	}
}

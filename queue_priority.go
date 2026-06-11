package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPriorityInvalid indicates an unsupported priority value.
	ErrPriorityInvalid = errors.New("priority queue: invalid priority")
	// ErrPriorityQueueStopped indicates the priority queue has been stopped.
	ErrPriorityQueueStopped = errors.New("priority queue: stopped")
	// ErrPriorityQueueFull indicates the target priority channel is full.
	ErrPriorityQueueFull = errors.New("priority queue: channel full")
)

const (
	defaultPriorityTick = 10 * time.Millisecond // Every ten milliseconds check for priority jobs.

	defaultWeightTotal   = 12 // Default total for percentage-to-number conversion (sum of default weights).
	defaultWeightHighest = 5  // Default weight for highest priority.
	defaultWeightHigh    = 3  // Default weight for high priority.
	defaultWeightMedium  = 2  // Default weight for medium priority.
	defaultWeightLow     = 1  // Default weight for low priority.
	defaultWeightLowest  = 1  // Default weight for lowest priority.
)

// NumberWeight represents a raw attempt count per dispatch tick.
type NumberWeight int

// PercentWeight represents a weight as a percentage (0-100).
// Percentages are converted to attempt counts using defaultWeightTotal.
type PercentWeight int

// weightConfig stores number of attempts per tick for each priority.
type weightConfig struct {
	highest int // Attempts for highest priority per tick.
	high    int // Attempts for high priority per tick.
	medium  int // Attempts for medium priority per tick.
	low     int // Attempts for low priority per tick.
	lowest  int // Attempts for lowest priority per tick.
}

// prioritySubmission preserves a submission while it waits in a priority buffer.
type prioritySubmission struct {
	job    Job
	meta   JobMeta
	handle *JobHandle
}

// PriorityQueueOption configures a PriorityQueue.
type PriorityQueueOption func(*PriorityQueue)

// Priority represents a priority tier in the priority queue.
type Priority int

const (
	PriorityLowest Priority = iota
	PriorityLow
	PriorityMedium
	PriorityHigh
	PriorityHighest
)

// String implements fmt.Stringer.
func (p Priority) String() string {
	return [5]string{"LOWEST", "LOW", "MEDIUM", "HIGH", "HIGHEST"}[p]
}

// PriorityQueue wraps a Queue with weighted priority dispatching.
type PriorityQueue struct {
	highest chan prioritySubmission // Highest priority buffer.
	high    chan prioritySubmission // High priority buffer.
	medium  chan prioritySubmission // Medium priority buffer.
	low     chan prioritySubmission // Low priority buffer.
	lowest  chan prioritySubmission // Lowest priority buffer.

	queue        *Queue        // Underlying queue where work is executed.
	priorityTick time.Duration // Dispatcher tick interval.
	weights      weightConfig  // Weighted forwarding config.

	ctx       context.Context    // Priority queue lifecycle context.
	ctxCancel context.CancelFunc // Cancels lifecycle context.
	stopped   atomic.Bool        // Indicates priority queue shutdown has started.

	wg             sync.WaitGroup          // Waits for dispatcher shutdown.
	acceptMut      sync.RWMutex            // Synchronizes acceptance with Stop.
	submissionsMut sync.Mutex              // Guards unresolved priority submissions.
	submissions    map[*JobHandle]struct{} // Submissions not yet forwarded or terminal.
}

// NewPriorityQueue creates a PriorityQueue around an existing Queue.
// capacity applies to each internal priority channel.
func NewPriorityQueue(queue *Queue, capacity int, opts ...PriorityQueueOption) *PriorityQueue {
	if queue == nil {
		panic("cq: priority queue requires base queue")
	}

	ctx, cancel := context.WithCancel(context.Background())
	pq := &PriorityQueue{
		highest:      make(chan prioritySubmission, capacity),
		high:         make(chan prioritySubmission, capacity),
		medium:       make(chan prioritySubmission, capacity),
		low:          make(chan prioritySubmission, capacity),
		lowest:       make(chan prioritySubmission, capacity),
		queue:        queue,
		priorityTick: defaultPriorityTick,
		weights: weightConfig{
			highest: defaultWeightHighest,
			high:    defaultWeightHigh,
			medium:  defaultWeightMedium,
			low:     defaultWeightLow,
			lowest:  defaultWeightLowest,
		},
		ctx:         ctx,
		ctxCancel:   cancel,
		submissions: make(map[*JobHandle]struct{}),
	}
	for _, opt := range opts {
		opt(pq)
	}

	pq.wg.Add(1)
	go pq.dispatcher()
	return pq
}

// Submit accepts one job into a priority buffer and returns its completion handle.
// ctx controls waiting for priority-buffer acceptance only. It does not cancel an accepted job.
func (pq *PriorityQueue) Submit(ctx context.Context, job Job, priority Priority, opts ...SubmitOption) (*JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ch, ok := pq.channelForPriority(priority)
	if !ok {
		return nil, ErrPriorityInvalid
	}
	pq.acceptMut.RLock()
	if pq.stopped.Load() {
		pq.acceptMut.RUnlock()
		return nil, ErrPriorityQueueStopped
	}
	select {
	case <-ctx.Done():
		pq.acceptMut.RUnlock()
		return nil, ctx.Err()
	case <-pq.ctx.Done():
		pq.acceptMut.RUnlock()
		return nil, ErrPriorityQueueStopped
	default:
	}

	cfg := resolveSubmitConfig(opts)
	meta := pq.queue.newSubmissionMeta(cfg)
	handle := newJobHandle(meta)
	submission := prioritySubmission{job: job, meta: meta, handle: handle}
	pq.trackSubmission(handle)
	pq.acceptMut.RUnlock()

	if cfg.nonBlocking {
		select {
		case ch <- submission:
			return handle, nil
		case <-ctx.Done():
			pq.untrackSubmission(handle)
			return nil, ctx.Err()
		case <-pq.ctx.Done():
			pq.untrackSubmission(handle)
			return nil, ErrPriorityQueueStopped
		default:
			pq.untrackSubmission(handle)
			return nil, ErrPriorityQueueFull
		}
	}

	select {
	case ch <- submission:
		return handle, nil
	case <-ctx.Done():
		pq.untrackSubmission(handle)
		return nil, ctx.Err()
	case <-pq.ctx.Done():
		pq.untrackSubmission(handle)
		return nil, ErrPriorityQueueStopped
	}
}

// SubmitAfter accepts responsibility for submitting a priority job after delay.
// When the delay elapses, priority-buffer acceptance is non-blocking.
func (pq *PriorityQueue) SubmitAfter(ctx context.Context, job Job, priority Priority, delay time.Duration, opts ...SubmitOption) (*JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ch, ok := pq.channelForPriority(priority)
	if !ok {
		return nil, ErrPriorityInvalid
	}
	pq.acceptMut.RLock()
	if pq.stopped.Load() {
		pq.acceptMut.RUnlock()
		return nil, ErrPriorityQueueStopped
	}
	select {
	case <-ctx.Done():
		pq.acceptMut.RUnlock()
		return nil, ctx.Err()
	case <-pq.ctx.Done():
		pq.acceptMut.RUnlock()
		return nil, ErrPriorityQueueStopped
	default:
	}
	cfg := resolveSubmitConfig(opts)
	meta := pq.queue.newSubmissionMeta(cfg)
	handle := newJobHandle(meta)
	submission := prioritySubmission{job: job, meta: meta, handle: handle}
	pq.trackSubmission(handle)
	pq.acceptMut.RUnlock()
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()
		select {
		case <-pq.ctx.Done():
			if handle.rejectPending(ErrPriorityQueueStopped) {
				pq.untrackSubmission(handle)
			}
		case <-handle.Done():
			pq.untrackSubmission(handle)
		case <-timer.C:
			select {
			case ch <- submission:
			case <-pq.ctx.Done():
				if handle.rejectPending(ErrPriorityQueueStopped) {
					pq.untrackSubmission(handle)
				}
			default:
				if handle.rejectPending(ErrPriorityQueueFull) {
					pq.untrackSubmission(handle)
				}
			}
		}
	}()
	return handle, nil
}

// CountByPriority returns queued count for one priority level.
// It returns 0 for invalid priorities.
func (pq *PriorityQueue) CountByPriority(priority Priority) int {
	ch, ok := pq.channelForPriority(priority)
	if !ok {
		return 0 // Invalid priority.
	}
	return len(ch)
}

// PendingByPriority returns queued counts for all priority levels.
func (pq *PriorityQueue) PendingByPriority() map[Priority]int {
	return map[Priority]int{
		PriorityHighest: len(pq.highest),
		PriorityHigh:    len(pq.high),
		PriorityMedium:  len(pq.medium),
		PriorityLow:     len(pq.low),
		PriorityLowest:  len(pq.lowest),
	}
}

// dispatcher periodically forwards jobs from priority buffers to base queue
// according to configured weighted attempts.
func (pq *PriorityQueue) dispatcher() {
	defer pq.wg.Done()
	tick := pq.priorityTick
	if tick <= 0 {
		tick = defaultPriorityTick
	}

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-pq.ctx.Done():
			return // Context is done, stop the dispatcher.
		case <-ticker.C:
			for i := 0; i < pq.weights.highest; i++ {
				pq.trySubmit(pq.highest)
			}
			for i := 0; i < pq.weights.high; i++ {
				pq.trySubmit(pq.high)
			}
			for i := 0; i < pq.weights.medium; i++ {
				pq.trySubmit(pq.medium)
			}
			for i := 0; i < pq.weights.low; i++ {
				pq.trySubmit(pq.low)
			}
			for i := 0; i < pq.weights.lowest; i++ {
				pq.trySubmit(pq.lowest)
			}
		}
	}
}

// trySubmit forwards a single submission from one priority channel to the base queue.
// On forward rejection, it best-effort requeues into the same channel.
func (pq *PriorityQueue) trySubmit(ch chan prioritySubmission) bool {
	select {
	case submission := <-ch:
		if submission.handle.state.Load() != submissionPending {
			pq.untrackSubmission(submission.handle)
			return false
		}
		ok, err := pq.queue.acceptSubmission(submission.job, submissionOptions{
			blocking:  false,
			acceptCtx: pq.ctx,
			meta:      submission.meta,
			handle:    submission.handle,
		})
		if ok {
			pq.untrackSubmission(submission.handle)
			return true
		}
		if errors.Is(err, ErrQueueStopped) || errors.Is(err, context.Canceled) {
			if submission.handle.rejectPending(err) {
				pq.untrackSubmission(submission.handle)
			}
			return false
		}

		select {
		case ch <- submission:
		default:
			if submission.handle.rejectPending(ErrPriorityQueueFull) {
				pq.untrackSubmission(submission.handle)
			}
		}
		return false
	default:
		return false
	}
}

// channelForPriority resolves the channel for a priority.
func (pq *PriorityQueue) channelForPriority(priority Priority) (chan prioritySubmission, bool) {
	switch priority {
	case PriorityHighest:
		return pq.highest, true
	case PriorityHigh:
		return pq.high, true
	case PriorityMedium:
		return pq.medium, true
	case PriorityLow:
		return pq.low, true
	case PriorityLowest:
		return pq.lowest, true
	default:
		return nil, false
	}
}

// Stop stops the dispatcher and optionally stops the underlying queue.
// Buffered and delayed priority submissions resolve with ErrPriorityQueueStopped.
func (pq *PriorityQueue) Stop(stopQueue bool) {
	pq.acceptMut.Lock()
	defer pq.acceptMut.Unlock()

	pq.stopped.Store(true)
	pq.ctxCancel()
	pq.wg.Wait()
	pq.rejectPendingSubmissions(ErrPriorityQueueStopped)
	if stopQueue {
		pq.queue.Stop(true)
	}
}

// Drain flushes buffered priority jobs into the underlying queue.
// It returns the number of jobs forwarded.
func (pq *PriorityQueue) Drain() int {
	drained := 0
	channels := []chan prioritySubmission{pq.highest, pq.high, pq.medium, pq.low, pq.lowest}

	for _, ch := range channels {
		for {
			select {
			case submission := <-ch:
				if submission.handle.state.Load() != submissionPending {
					pq.untrackSubmission(submission.handle)
					continue
				}
				ok, err := pq.queue.acceptSubmission(submission.job, submissionOptions{
					blocking:  true,
					acceptCtx: context.Background(),
					meta:      submission.meta,
					handle:    submission.handle,
				})
				if ok {
					pq.untrackSubmission(submission.handle)
					drained++
				} else if submission.handle.rejectPending(err) {
					pq.untrackSubmission(submission.handle)
				}
			default:
				goto nextChannel
			}
		}
	nextChannel:
	}

	return drained
}

// trackSubmission records a submission waiting in a priority buffer or delay.
func (pq *PriorityQueue) trackSubmission(handle *JobHandle) {
	pq.submissionsMut.Lock()
	pq.submissions[handle] = struct{}{}
	pq.submissionsMut.Unlock()
}

// untrackSubmission removes a submission forwarded to the underlying queue or made terminal.
func (pq *PriorityQueue) untrackSubmission(handle *JobHandle) {
	pq.submissionsMut.Lock()
	delete(pq.submissions, handle)
	pq.submissionsMut.Unlock()
}

// rejectPendingSubmissions resolves every submission still owned by the priority queue.
func (pq *PriorityQueue) rejectPendingSubmissions(err error) {
	pq.submissionsMut.Lock()
	defer pq.submissionsMut.Unlock()
	for handle := range pq.submissions {
		handle.rejectPending(err)
		delete(pq.submissions, handle)
	}
}

// WithPriorityTick sets dispatcher tick interval.
func WithPriorityTick(tick time.Duration) PriorityQueueOption {
	return func(pq *PriorityQueue) {
		if tick <= 0 {
			pq.priorityTick = defaultPriorityTick
			return
		}
		pq.priorityTick = tick
	}
}

// WithWeighting sets weighted attempts per priority tier per tick.
// Inputs support NumberWeight, PercentWeight, or raw int.
func WithWeighting(highest, high, medium, low, lowest interface{}) PriorityQueueOption {
	return func(pq *PriorityQueue) {
		convertWeight := func(w interface{}) int {
			switch v := w.(type) {
			case NumberWeight:
				return int(v)
			case PercentWeight:
				return max((defaultWeightTotal*int(v))/100, 1)
			case int:
				return v
			default:
				return 1
			}
		}

		pq.weights = weightConfig{
			highest: convertWeight(highest),
			high:    convertWeight(high),
			medium:  convertWeight(medium),
			low:     convertWeight(low),
			lowest:  convertWeight(lowest),
		}
	}
}

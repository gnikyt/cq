package cq

import (
	"context"
	"errors"
	"sync"
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
	highest chan Job // Highest priority buffer.
	high    chan Job // High priority buffer.
	medium  chan Job // Medium priority buffer.
	low     chan Job // Low priority buffer.
	lowest  chan Job // Lowest priority buffer.

	queue        *Queue        // Underlying queue where work is executed.
	priorityTick time.Duration // Dispatcher tick interval.
	weights      weightConfig  // Weighted forwarding config.

	ctx       context.Context    // Priority queue lifecycle context.
	ctxCancel context.CancelFunc // Cancels lifecycle context.

	wg sync.WaitGroup // Waits for dispatcher shutdown.
}

// NewPriorityQueue creates a PriorityQueue around an existing Queue.
// capacity applies to each internal priority channel.
func NewPriorityQueue(queue *Queue, capacity int, opts ...PriorityQueueOption) *PriorityQueue {
	ctx, cancel := context.WithCancel(context.Background())
	pq := &PriorityQueue{
		highest:      make(chan Job, capacity),
		high:         make(chan Job, capacity),
		medium:       make(chan Job, capacity),
		low:          make(chan Job, capacity),
		lowest:       make(chan Job, capacity),
		queue:        queue,
		priorityTick: defaultPriorityTick,
		weights: weightConfig{
			highest: defaultWeightHighest,
			high:    defaultWeightHigh,
			medium:  defaultWeightMedium,
			low:     defaultWeightLow,
			lowest:  defaultWeightLowest,
		},
		ctx:       ctx,
		ctxCancel: cancel,
	}
	for _, opt := range opts {
		opt(pq)
	}

	pq.wg.Add(1)
	go pq.dispatcher()
	return pq
}

// Enqueue blocks until the job is accepted or queue stops.
// Invalid priorities fallback to medium priority channel.
func (pq *PriorityQueue) Enqueue(job Job, priority Priority) {
	_ = pq.EnqueueOrError(job, priority)
}

// EnqueueOrError blocks until accepted or stopped.
// Invalid priorities fallback to medium priority channel.
func (pq *PriorityQueue) EnqueueOrError(job Job, priority Priority) error {
	ch, ok := pq.channelForPriority(priority, true)
	if !ok {
		return ErrPriorityInvalid // Invalid priority.
	}

	select {
	case <-pq.ctx.Done():
		return ErrPriorityQueueStopped // Queue is stopped.
	default:
	}

	select {
	case ch <- job:
		return nil // Job added to priority channel.
	case <-pq.ctx.Done():
		return ErrPriorityQueueStopped // Queue is stopped.
	}
}

// TryEnqueue attempts to enqueue without blocking.
// It returns false when priority is invalid, queue is stopped, or channel is full.
func (pq *PriorityQueue) TryEnqueue(job Job, priority Priority) bool {
	ok, _ := pq.TryEnqueueOrError(job, priority)
	return ok
}

// TryEnqueueOrError attempts to enqueue without blocking.
// It returns typed errors for invalid priority, stopped queue, and full channel.
func (pq *PriorityQueue) TryEnqueueOrError(job Job, priority Priority) (bool, error) {
	ch, ok := pq.channelForPriority(priority, false)
	if !ok {
		return false, ErrPriorityInvalid // Invalid priority.
	}

	select {
	case <-pq.ctx.Done():
		return false, ErrPriorityQueueStopped // Queue is stopped.
	default:
	}

	select {
	case ch <- job:
		return true, nil // Job added to priority channel.
	case <-pq.ctx.Done():
		return false, ErrPriorityQueueStopped // Queue is stopped.
	default:
		return false, ErrPriorityQueueFull // Channel is full.
	}
}

// DelayEnqueue enqueues a job after delay in a separate goroutine.
func (pq *PriorityQueue) DelayEnqueue(job Job, priority Priority, delay time.Duration) {
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()
		select {
		case <-pq.ctx.Done():
			return // Context is done, stop the timer.
		case <-timer.C:
			pq.Enqueue(job, priority)
		}
	}()
}

// CountByPriority returns queued count for one priority level.
// It returns 0 for invalid priorities.
func (pq *PriorityQueue) CountByPriority(priority Priority) int {
	ch, ok := pq.channelForPriority(priority, false)
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
	ticker := time.NewTicker(pq.priorityTick)
	defer ticker.Stop()

	for {
		select {
		case <-pq.ctx.Done():
			return // Context is done, stop the dispatcher.
		case <-ticker.C:
			for i := 0; i < pq.weights.highest; i++ {
				pq.tryEnqueue(pq.highest)
			}
			for i := 0; i < pq.weights.high; i++ {
				pq.tryEnqueue(pq.high)
			}
			for i := 0; i < pq.weights.medium; i++ {
				pq.tryEnqueue(pq.medium)
			}
			for i := 0; i < pq.weights.low; i++ {
				pq.tryEnqueue(pq.low)
			}
			for i := 0; i < pq.weights.lowest; i++ {
				pq.tryEnqueue(pq.lowest)
			}
		}
	}
}

// tryEnqueue forwards a single job from one priority channel to base queue.
// On forward rejection, it best-effort requeues into the same channel.
func (pq *PriorityQueue) tryEnqueue(ch chan Job) bool {
	select {
	case job := <-ch:
		ok, err := pq.queue.TryEnqueueOrError(job)
		if ok && err == nil {
			return true // Job forwarded to base queue.
		}

		select {
		case ch <- job:
		default:
		}
		return false // Job not forwarded to base queue.
	default:
		return false // Channel is empty.
	}
}

// channelForPriority resolves the channel for a priority.
// If fallbackMedium is true, invalid priorities map to medium.
func (pq *PriorityQueue) channelForPriority(priority Priority, fallbackMedium bool) (chan Job, bool) {
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
		if fallbackMedium {
			return pq.medium, true
		}
		return nil, false
	}
}

// Stop stops the dispatcher and optionally stops the underlying queue.
// Buffered priority jobs are dropped unless Drain is called first.
func (pq *PriorityQueue) Stop(stopQueue bool) {
	pq.ctxCancel()
	pq.wg.Wait()
	if stopQueue {
		pq.queue.Stop(true)
	}
}

// Drain flushes buffered priority jobs into the underlying queue.
// It returns the number of jobs forwarded.
func (pq *PriorityQueue) Drain() int {
	drained := 0
	channels := []chan Job{pq.highest, pq.high, pq.medium, pq.low, pq.lowest}

	for _, ch := range channels {
		for {
			select {
			case job := <-ch:
				pq.queue.Enqueue(job)
				drained++
			default:
				goto nextChannel
			}
		}
	nextChannel:
	}

	return drained
}

// WithPriorityTick sets dispatcher tick interval.
func WithPriorityTick(tick time.Duration) PriorityQueueOption {
	return func(pq *PriorityQueue) {
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

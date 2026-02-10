package cq

import (
	"context"
	"sync"
	"time"
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
// Percentages are converted to attempt counts using a fixed total.
type PercentWeight int

// weightConfig stores the number of attempts per tick for each priority level.
type weightConfig struct {
	highest int // Number of attempts for highest priority per tick.
	high    int // Number of attempts for high priority per tick.
	medium  int // Number of attempts for medium priority per tick.
	low     int // Number of attempts for low priority per tick.
	lowest  int // Number of attempts for lowest priority per tick.
}

// Functional options for PriorityQueue.
type PriorityQueueOption func(*PriorityQueue)

// Priority levels for job dispatch.
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

// PriorityQueue wraps a Queue with priority-based job ordering.
// Jobs are dispatched from highest to lowest priority channels.
type PriorityQueue struct {
	highest chan Job // Highest priority channel.
	high    chan Job // High priority channel.
	medium  chan Job // Medium priority channel.
	low     chan Job // Low priority channel.
	lowest  chan Job // Lowest priority channel.

	queue        *Queue        // Underlying queue.
	priorityTick time.Duration // Tick duration for priority dispatcher.
	weights      weightConfig  // Weight configuration for priority attempts per tick.

	ctx       context.Context    // Context.
	ctxCancel context.CancelFunc // Context cancel function.

	wg sync.WaitGroup // Wait group.
}

// NewPriorityQueue creates a PriorityQueue that wraps the provided Queue.
// `capacity` sets the channel buffer size for each priority level.
// Additional options can be passed via `opts`.
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

// Enqueue submits a job to the selected priority channel.
// Invalid priorities default to `PriorityMedium`.
func (pq *PriorityQueue) Enqueue(job Job, priority Priority) {
	var ch chan Job
	switch priority {
	case PriorityHighest:
		ch = pq.highest
	case PriorityHigh:
		ch = pq.high
	case PriorityMedium:
		ch = pq.medium
	case PriorityLow:
		ch = pq.low
	case PriorityLowest:
		ch = pq.lowest
	default:
		ch = pq.medium
	}

	select {
	case ch <- job:
		// Job added to priority channel.
	case <-pq.ctx.Done():
		// Context cancelled, discard job.
	}
}

// TryEnqueue attempts to submit a job without blocking.
// Returns true if accepted, or false if the channel is full, closed, or priority is invalid.
func (pq *PriorityQueue) TryEnqueue(job Job, priority Priority) bool {
	var ch chan Job
	switch priority {
	case PriorityHighest:
		ch = pq.highest
	case PriorityHigh:
		ch = pq.high
	case PriorityMedium:
		ch = pq.medium
	case PriorityLow:
		ch = pq.low
	case PriorityLowest:
		ch = pq.lowest
	default:
		return false
	}

	select {
	case ch <- job:
		return true
	case <-pq.ctx.Done():
		return false
	default:
		return false
	}
}

// DelayEnqueue submits a job to the priority queue after the specified delay.
// The delay runs in its own goroutine so the caller is not blocked.
func (pq *PriorityQueue) DelayEnqueue(job Job, priority Priority, delay time.Duration) {
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()
		select {
		case <-pq.ctx.Done():
			return // Context cancelled, stop delay.
		case <-timer.C:
			pq.Enqueue(job, priority) // Job can be submitted now.
		}
	}()
}

// CountByPriority returns pending jobs for the specified priority.
// Returns 0 for invalid priorities.
func (pq *PriorityQueue) CountByPriority(priority Priority) int {
	var ch chan Job
	switch priority {
	case PriorityHighest:
		ch = pq.highest
	case PriorityHigh:
		ch = pq.high
	case PriorityMedium:
		ch = pq.medium
	case PriorityLow:
		ch = pq.low
	case PriorityLowest:
		ch = pq.lowest
	default:
		return 0
	}
	return len(ch)
}

// PendingByPriority returns pending job counts for all priority levels.
func (pq *PriorityQueue) PendingByPriority() map[Priority]int {
	return map[Priority]int{
		PriorityHighest: len(pq.highest),
		PriorityHigh:    len(pq.high),
		PriorityMedium:  len(pq.medium),
		PriorityLow:     len(pq.low),
		PriorityLowest:  len(pq.lowest),
	}
}

// dispatcher pulls jobs from priority channels and forwards them to the underlying queue.
// Each priority gets weighted attempts per tick based on the configured weights.
func (pq *PriorityQueue) dispatcher() {
	defer pq.wg.Done()
	ticker := time.NewTicker(pq.priorityTick)
	defer ticker.Stop()

	for {
		select {
		case <-pq.ctx.Done():
			return // Context cancelled, stop dispatcher.
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

// tryEnqueue attempts to pull one job from `ch` and enqueue it.
// Returns true if a job was forwarded, false otherwise.
func (pq *PriorityQueue) tryEnqueue(ch chan Job) bool {
	select {
	case job := <-ch:
		pq.queue.Enqueue(job)
		return true
	default:
		return false
	}
}

// Stop stops the priority dispatcher and waits for it to finish.
// If `stopQueue` is true, it also stops the underlying queue.
// Jobs still buffered in priority channels are dropped.
func (pq *PriorityQueue) Stop(stopQueue bool) {
	pq.ctxCancel()
	pq.wg.Wait()

	if stopQueue {
		pq.queue.Stop(true)
	}
}

// WithPriorityTick sets the dispatcher tick interval.
func WithPriorityTick(tick time.Duration) PriorityQueueOption {
	return func(pq *PriorityQueue) {
		pq.priorityTick = tick
	}
}

// WithWeighting sets custom per-priority dispatch weights.
// Weights determine attempts per tick for each priority level.
// Accepts NumberWeight, PercentWeight, or raw int values.
func WithWeighting(highest, high, medium, low, lowest interface{}) PriorityQueueOption {
	return func(pq *PriorityQueue) {
		convertWeight := func(w interface{}) int {
			switch v := w.(type) {
			case NumberWeight:
				return int(v)
			case PercentWeight:
				return max((defaultWeightTotal*int(v))/100, 1) // Convert percentage to count based on default total.
			case int:
				return v // Support raw int, no need to use NumberWeight specifically.
			default:
				return 1 // Fallback to 1 attempt per tick.
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

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

// NumberWeight represents a weight as a raw count of attempts per tick.
type NumberWeight int

// PercentWeight represents a weight as a percentage (0-100).
// Percentages are converted to counts based on a fixed total.
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

// Priority levels for job execution.
type Priority int

const (
	PriorityLowest Priority = iota
	PriorityLow
	PriorityMedium
	PriorityHigh
	PriorityHighest
)

// String() implementation.
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

// NewPriorityQueue creates a new PriorityQueue that wraps the provided Queue.
// The capacity parameter sets the buffer size for each priority level.
// Additional configuration options can be passed in as the remaining parameters.
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

// Enqueue adds a job to the priority queue with the specified priority level.
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

// TryEnqueue attempts to add a job to the priority queue without blocking.
// Returns true if the job was successfully added, false if the priority channel is full.
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

// DelayEnqueue adds a job to the priority queue after the specified delay.
// It runs in its own goroutine to avoid blocking.
func (pq *PriorityQueue) DelayEnqueue(job Job, priority Priority, delay time.Duration) {
	go func() {
		<-time.After(delay)
		pq.Enqueue(job, priority)
	}()
}

// CountByPriority returns the number of pending jobs in the specified priority level.
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

// PendingByPriority returns a map of all priority levels with their pending job counts.
func (pq *PriorityQueue) PendingByPriority() map[Priority]int {
	return map[Priority]int{
		PriorityHighest: len(pq.highest),
		PriorityHigh:    len(pq.high),
		PriorityMedium:  len(pq.medium),
		PriorityLow:     len(pq.low),
		PriorityLowest:  len(pq.lowest),
	}
}

// dispatcher pulls jobs from priority channels and enqueues them
// to the underlying queue using weighted attempts per tick.
// Higher priority levels get more attempts per tick based on weight configuration.
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

// tryEnqueue attempts to pull a job from the given channel and enqueue it.
// Returns true if a job was enqueued, false otherwise.
func (pq *PriorityQueue) tryEnqueue(ch chan Job) bool {
	select {
	case job := <-ch:
		pq.queue.Enqueue(job)
		return true
	default:
		return false
	}
}

// Stop stops the priority queue dispatcher and waits for it to finish.
// If stopQueue is true, it will also stop the underlying queue.
// Any jobs remaining in the priority channels will not be processed.
func (pq *PriorityQueue) Stop(stopQueue bool) {
	pq.ctxCancel()
	pq.wg.Wait()
	if stopQueue {
		pq.queue.Stop(true)
	}
}

// WithPriorityTick is a functional option for PriorityQueue to
// set the tick duration for the priority dispatcher.
func WithPriorityTick(tick time.Duration) PriorityQueueOption {
	return func(pq *PriorityQueue) {
		pq.priorityTick = tick
	}
}

// WithWeighting is a functional option for PriorityQueue to set custom weights
// for each priority level. Weights determine how many attempts per tick each
// priority level gets. Accepts both NumberWeight (raw number) and PercentWeight
// (percentage that converts to a raw number).
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

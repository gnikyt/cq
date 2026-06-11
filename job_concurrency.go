package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const defaultConcurrencyRetryDelay = 50 * time.Millisecond

var (
	ErrConcurrencyJobRequired     = errors.New("cq: concurrency job required")
	ErrConcurrencyLimiterRequired = errors.New("cq: concurrency limiter required")
	ErrConcurrencyQueueRequired   = errors.New("cq: concurrency queue required")
)

// concurrencyEntry holds the atomic active count for one key.
type concurrencyEntry struct {
	current atomic.Int32
}

// ConcurrencyLimiter tracks active-job counts per key.
// One instance may be shared by multiple WithConcurrencyLimit wrappers
// by using different keys.
type ConcurrencyLimiter struct {
	mu      sync.Mutex
	entries map[string]*concurrencyEntry
}

// NewConcurrencyLimiter creates a new, ready-to-use ConcurrencyLimiter.
func NewConcurrencyLimiter() *ConcurrencyLimiter {
	return &ConcurrencyLimiter{entries: make(map[string]*concurrencyEntry)}
}

// acquire attempts to claim one slot for key against max using CAS.
// Returns (entry, true) on success.
// Retrutns (nil, false) when at limit.
func (cl *ConcurrencyLimiter) acquire(key string, max int) (*concurrencyEntry, bool) {
	cl.mu.Lock()
	e, ok := cl.entries[key]
	if !ok {
		// Create entry for the key.
		e = &concurrencyEntry{}
		cl.entries[key] = e
	}
	cl.mu.Unlock()

	for {
		cur := e.current.Load()
		if int(cur) >= max {
			return nil, false // Unable to acquire.
		}
		if e.current.CompareAndSwap(cur, cur+1) {
			return e, true // Acquired successfully.
		}
		// Lost CAS race... retry.
	}
}

// release decrements the count for the entry returned by acquire.
func (cl *ConcurrencyLimiter) release(e *concurrencyEntry) {
	e.current.Add(-1)
}

// ActiveFor returns the current active count for key (0 if never used).
func (cl *ConcurrencyLimiter) ActiveFor(key string) int {
	cl.mu.Lock()
	e, ok := cl.entries[key]
	cl.mu.Unlock()
	if !ok {
		return 0
	}
	return int(e.current.Load())
}

// WithConcurrencyLimit limits how many jobs sharing key can execute concurrently.
// When the limit is reached the job is resubmitted after retryDelay and the
// current worker is freed immediately (returns nil).
func WithConcurrencyLimit(job Job, key string, max int, retryDelay time.Duration, limiter *ConcurrencyLimiter, queue *Queue) Job {
	if max < 1 {
		max = 1
	}
	if retryDelay <= 0 {
		retryDelay = defaultConcurrencyRetryDelay
	}

	var wrappedJob Job
	wrappedJob = func(ctx context.Context) error {
		if job == nil {
			return ErrConcurrencyJobRequired
		}
		if limiter == nil {
			return ErrConcurrencyLimiterRequired
		}
		if queue == nil {
			return ErrConcurrencyQueueRequired
		}

		entry, ok := limiter.acquire(key, max)
		if !ok {
			// At limit: free this worker, schedule retry.
			_, err := Reschedule(ctx, queue, wrappedJob, retryDelay, RescheduleReasonConcurrencyLimit)
			return err
		}
		defer limiter.release(entry)
		return job(ctx)
	}
	return wrappedJob
}

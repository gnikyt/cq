package cq

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrUniqueContended         = errors.New("cq: unique contention detected")
	ErrWithoutOverlapContended = errors.New("cq: without overlap contention detected")
)

// uniqueContentionOrDiscard returns ErrUniqueContended under ContextWithContentionTry.
// Otherwise, it returns nil (quiet discard).
func uniqueContentionOrDiscard(ctx context.Context) error {
	if contentionTryFromContext(ctx) {
		return ErrUniqueContended
	}
	return nil
}

// WithoutOverlap ensures multiple jobs of a given key cannot run concurrently.
// When ctx carries contention-try mode (see ContextWithContentionTry),
// WithoutOverlap uses TryLock instead: if the mutex is busy it returns
// ErrWithoutOverlapContended without running the inner job.
// WithDispatchOnContention/WithDispatchOnError sets this automatically when invoking the job.
func WithoutOverlap(job Job, key string, locker Locker[*sync.Mutex]) Job {
	return func(ctx context.Context) error {
		locker.Acquire(key, LockValue[*sync.Mutex]{
			ExpiresAt: time.Time{},
			Value:     &sync.Mutex{},
		})

		lock, _ := locker.Get(key)
		mut := lock.Value

		if contentionTryFromContext(ctx) {
			// Contention-try mode: try to lock the mutex.
			if mut.TryLock() {
				defer mut.Unlock()
				return job(ctx)
			}
			return ErrWithoutOverlapContended // Mutex is busy, return the contention error.
		}

		// Regular mode: lock the mutex.
		mut.Lock()
		defer mut.Unlock()
		return job(ctx)
	}
}

// WithUnique ensures only one job of a given key runs within a time window.
// If a job with the same key already ran within the duration (ut), duplicate
// jobs are discarded (returns nil). The lock is released when the job completes
// or when the duration expires, whichever comes first.
// If ut is zero, the lock persists until the job completes, ensuring only
// one instance can run at a time without any time-based constraint.
// For enforcing a fixed minimum time between executions regardless of job
// completion time, use WithUniqueWindow instead.
// Under WithDispatchOnContention/WithDispatchOnError, duplicate runs return
// ErrUniqueContended so the outer wrapper can apply drop, error, dispatch, or block policy.
func WithUnique(job Job, key string, ut time.Duration, locker Locker[struct{}]) Job {
	return func(ctx context.Context) error {
		lock, exists := locker.Get(key)
		if exists {
			if !lock.IsExpired() {
				return uniqueContentionOrDiscard(ctx)
			}
			// Lock exists, but is expired, release it. In this event, the job may have
			// not been processed yet, took too long to complete, etc.
			locker.Release(key)
		}

		// Lock either does not exist or was released... acquire a new lock.
		var es struct{}
		var expiresAt time.Time
		if ut == 0 {
			// Zero duration means no expiration (lock until job completes).
			expiresAt = time.Time{}
		} else {
			// Append duration to now.
			expiresAt = time.Now().Add(ut)
		}
		if !locker.Acquire(key, LockValue[struct{}]{
			ExpiresAt: expiresAt,
			Value:     es,
		}) {
			return uniqueContentionOrDiscard(ctx) // Lock acquisition failed, return the contention error.
		}

		defer locker.Release(key)
		return job(ctx)
	}
}

// WithUniqueWindow ensures a job can only run once within a fixed time window,
// regardless of how quickly the job completes. Unlike WithUnique, the lock is
// not released when the job completes... instead, it persists for the full duration.
// This guarantees a minimum time gap between executions.
// Under WithDispatchOnContention, duplicate runs return ErrUniqueContended so the outer
// wrapper can apply policy.
func WithUniqueWindow(job Job, key string, window time.Duration, locker Locker[struct{}]) Job {
	return func(ctx context.Context) error {
		lock, exists := locker.Get(key)
		if exists {
			if !lock.IsExpired() {
				return uniqueContentionOrDiscard(ctx)
			}
			// Lock exists, but is expired, release it. In this event, the job may have
			// not been processed yet, took too long to complete, etc.
			locker.Release(key)
		}

		// Acquire lock for the full window duration.
		if !locker.Acquire(key, LockValue[struct{}]{
			ExpiresAt: time.Now().Add(window),
			Value:     struct{}{},
		}) {
			return uniqueContentionOrDiscard(ctx) // Lock acquisition failed, return the contention error.
		}
		return job(ctx)
	}
}

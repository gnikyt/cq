package cq

import (
	"context"
	"sync"
	"time"
)

// WithoutOverlap ensures multiple jobs of a given key cannot run concurrently.
// This is useful when jobs touch shared data (such asaccount balances) and must
// execute sequentially to prevent race conditions.
// Jobs with the same key will block workers while waiting for the mutex.
// If many jobs with the same key are enqueued, workers become tied up waiting,
// potentially exhausting the worker pool. For high-volume deduplication where only
// the first job matters, use WithUnique instead to discard duplicates without blocking.
func WithoutOverlap(job Job, key string, locker Locker[*sync.Mutex]) Job {
	return func(ctx context.Context) error {
		// Ensure a lock exists for this key.
		locker.Aquire(key, LockValue[*sync.Mutex]{
			ExpiresAt: time.Time{},
			Value:     &sync.Mutex{},
		})

		// Get the lock, whoever won the race will have the lock.
		lock, _ := locker.Get(key)
		mut := lock.Value
		mut.Lock()
		defer mut.Unlock()

		return job(ctx)
	}
}

// WithUnique ensures only one job of a given key runs within a time window.
// If a job with the same key already ran within the duration (ut), duplicate
// jobs are discarded. The lock is released when the job completes or when the
// duration expires, whichever comes first.
// If ut is zero, the lock persists until the job completes, ensuring only
// one instance can run at a time without any time-based constraint.
// For enforcing a fixed minimum time between executions regardless of job
// completion time, use WithUniqueWindow instead.
func WithUnique(job Job, key string, ut time.Duration, locker Locker[struct{}]) Job {
	return func(ctx context.Context) error {
		lock, exists := locker.Get(key)
		if exists {
			if !lock.IsExpired() {
				return nil // Return this job as "done" since original job has not yet completed.
			} else {
				// Lock exists, but is expired, release it. In this event, the job may have
				// not been processed yet, took too long to complete, etc.
				locker.Release(key)
			}
		}
		// Lock either doesn't exist or was released, aquire a new lock.
		var es struct{}
		var expiresAt time.Time
		if ut == 0 {
			// Zero duration means no expiration (lock until job completes).
			expiresAt = time.Time{}
		} else {
			// Append duration to now.
			expiresAt = time.Now().Add(ut)
		}
		locker.Aquire(key, LockValue[struct{}]{
			ExpiresAt: expiresAt,
			Value:     es,
		})
		defer locker.Release(key)
		return job(ctx)
	}
}

// WithUniqueWindow ensures a job can only run once within a fixed time window,
// regardless of how quickly the job completes. Unlike WithUnique, the lock is
// not released when the job completes... instead, it persists for the full duration.
// This guarantees a minimum time gap between executions.
func WithUniqueWindow(job Job, key string, window time.Duration, locker Locker[struct{}]) Job {
	return func(ctx context.Context) error {
		lock, exists := locker.Get(key)
		if exists {
			if !lock.IsExpired() {
				return nil // Lock still active, discard this job.
			} else {
				// Lock expired, release it.
				locker.Release(key)
			}
		}
		// Aquire lock for the full window duration.
		locker.Aquire(key, LockValue[struct{}]{
			ExpiresAt: time.Now().Add(window),
			Value:     struct{}{},
		})
		return job(ctx)
	}
}

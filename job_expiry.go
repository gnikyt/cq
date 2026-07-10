package cq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrJobExpired is returned (wrapped as a discard outcome) when a job's
// queued wait exceeded its expiry and it was not run.
var ErrJobExpired = errors.New("cq: job expired")

// WithExpiry discards a job that does not start executing within ttl of
// being enqueued, bounding waiting rather than running. Expired jobs are
// discard outcomes (errors.Is reports both ErrJobExpired and ErrDiscard),
// counting as discarded rather than failed.
//
// The enqueue time is read from JobMeta in context. When ttl is not positive
// or no enqueue time is available, the job runs normally.
func WithExpiry(job Job, ttl time.Duration) Job {
	if ttl <= 0 {
		return job
	}

	return func(ctx context.Context) error {
		meta := MetaFromContext(ctx)
		if meta.EnqueuedAt.IsZero() {
			return job(ctx) // No enqueue time available... run job normally.
		}
		if waited := time.Since(meta.EnqueuedAt); waited > ttl {
			return AsDiscard(fmt.Errorf("%w: waited %s with expiry of %s", ErrJobExpired, waited.Round(time.Millisecond), ttl))
		}
		return job(ctx)
	}
}

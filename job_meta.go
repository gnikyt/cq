package cq

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrTouchLockUnavailable is returned when TouchLock is called outside a unique
	// execution context that exposes lock-touch capability.
	ErrTouchLockUnavailable = errors.New("cq: touch lock unavailable in context")
	// ErrUniqueLeaseLost is returned when a unique-window lock lease cannot be renewed.
	ErrUniqueLeaseLost = errors.New("cq: unique lease lost")
)

// jobMetaKey is the context key for job metadata.
type jobMetaKey struct{}

// releaseRequesterKey is the context key for the release requester.
type releaseRequesterKey struct{}

// releaseRequester is a function that requests a release of a job after a delay.
type releaseRequester func(time.Duration) bool

// lockTouchRequesterKey is the context key for unique lock touch requester.
type lockTouchRequesterKey struct{}

// lockTouchRequester is a function that requests a unique lock touch.
type lockTouchRequester func(time.Duration) error

// lastErrorKey is the context key for the previous attempt error.
type lastErrorKey struct{}

// JobMeta contains metadata about the current job execution.
type JobMeta struct {
	ID         string            // Unique job identifier.
	Name       string            // Optional human-readable job name.
	Attributes map[string]string // Optional string attributes for correlation and observability.
	EnqueuedAt time.Time         // When the job was enqueued.
	Attempt    int               // Current attempt (0-indexed, externally incremented).
}

// MetaFromContext extracts job metadata from the context.
// Returns an empty JobMeta if no metadata is present.
func MetaFromContext(ctx context.Context) JobMeta {
	if meta, ok := ctx.Value(jobMetaKey{}).(JobMeta); ok {
		return cloneJobMeta(meta)
	}
	return JobMeta{}
}

// contextWithMeta returns a new context with the given job metadata.
func contextWithMeta(ctx context.Context, meta JobMeta) context.Context {
	return context.WithValue(ctx, jobMetaKey{}, cloneJobMeta(meta))
}

// LastErrorFromContext extracts the previous attempt error from context.
// Returns nil when no previous attempt error is present.
func LastErrorFromContext(ctx context.Context) error {
	if err, ok := ctx.Value(lastErrorKey{}).(error); ok {
		return err
	}
	return nil
}

// contextWithLastError returns a new context with the previous attempt error.
func contextWithLastError(ctx context.Context, err error) context.Context {
	return context.WithValue(ctx, lastErrorKey{}, err)
}

// RequestRelease asks the current wrapper chain to resubmit this job after delay.
// Returns false when no release-self wrapper is present or metadata is unavailable.
func RequestRelease(ctx context.Context, delay time.Duration) bool {
	fn, ok := ctx.Value(releaseRequesterKey{}).(releaseRequester)
	if !ok || fn == nil {
		return false
	}
	return fn(delay)
}

// contextWithReleaseRequester returns a new context with release-self requester.
func contextWithReleaseRequester(ctx context.Context, fn releaseRequester) context.Context {
	return context.WithValue(ctx, releaseRequesterKey{}, fn)
}

// TouchLock requests the current unique lock lease to be extended by ttl.
// Returns ErrTouchLockUnavailable when no unique lock touch requester is present.
func TouchLock(ctx context.Context, ttl time.Duration) error {
	fn, ok := ctx.Value(lockTouchRequesterKey{}).(lockTouchRequester)
	if !ok || fn == nil {
		return ErrTouchLockUnavailable
	}
	return fn(ttl)
}

// contextWithLockTouchRequester returns a new context with unique lock touch requester.
func contextWithLockTouchRequester(ctx context.Context, fn lockTouchRequester) context.Context {
	return context.WithValue(ctx, lockTouchRequesterKey{}, fn)
}

// cloneJobMeta returns JobMeta with an independent Attributes map.
func cloneJobMeta(meta JobMeta) JobMeta {
	meta.Attributes = cloneStringMap(meta.Attributes)
	return meta
}

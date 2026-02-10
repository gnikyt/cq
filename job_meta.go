package cq

import (
	"context"
	"time"
)

// jobMetaKey is the context key for job metadata.
type jobMetaKey struct{}

// JobMeta contains metadata about the current job execution.
type JobMeta struct {
	ID         string    // Unique job identifier.
	EnqueuedAt time.Time // When the job was enqueued.
	Attempt    int       // Current attempt (0-indexed, externally incremented).
}

// MetaFromContext extracts job metadata from the context.
// Returns an empty JobMeta if no metadata is present.
func MetaFromContext(ctx context.Context) JobMeta {
	if meta, ok := ctx.Value(jobMetaKey{}).(JobMeta); ok {
		return meta
	}
	return JobMeta{}
}

// contextWithMeta returns a new context with the given job metadata.
func contextWithMeta(ctx context.Context, meta JobMeta) context.Context {
	return context.WithValue(ctx, jobMetaKey{}, meta)
}

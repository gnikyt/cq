package cq

import (
	"context"
	"time"
)

// jobMetaKey is the context key for job metadata.
type jobMetaKey struct{}

// releaseRequesterKey is the context key for the release requester.
type releaseRequesterKey struct{}

// releaseRequester is a function that requests a release of a job after a delay.
type releaseRequester func(time.Duration) bool

// envelopePayloadSetterKey is the context key for runtime envelope payload updates.
type envelopePayloadSetterKey struct{}

// envelopePayloadSetter stores replay envelope metadata for the current run.
type envelopePayloadSetter func(typ string, payload []byte) bool

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

// RequestRelease asks the current wrapper chain to re-enqueue this job after delay.
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

// SetEnvelopePayload stores replay metadata (type + payload) for the current job run.
// Returns false when no envelope payload sink is configured for this context.
func SetEnvelopePayload(ctx context.Context, typ string, payload []byte) bool {
	fn, ok := ctx.Value(envelopePayloadSetterKey{}).(envelopePayloadSetter)
	if !ok || fn == nil {
		return false
	}
	return fn(typ, payload)
}

// contextWithEnvelopePayloadSetter returns a new context with envelope payload setter.
func contextWithEnvelopePayloadSetter(ctx context.Context, fn envelopePayloadSetter) context.Context {
	return context.WithValue(ctx, envelopePayloadSetterKey{}, fn)
}

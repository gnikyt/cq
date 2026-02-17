package cq

import (
	"context"
	"time"
)

// EnvelopeStatus represents persisted lifecycle state for a job envelope.
type EnvelopeStatus string

const (
	EnvelopeStatusEnqueued EnvelopeStatus = "enqueued"
	EnvelopeStatusClaimed  EnvelopeStatus = "claimed"
	EnvelopeStatusAcked    EnvelopeStatus = "acked"
	EnvelopeStatusNacked   EnvelopeStatus = "nacked"
)

// Reschedule reasons used by built-in wrappers.
const (
	EnvelopeRescheduleReasonRelease     = "release"
	EnvelopeRescheduleReasonReleaseSelf = "release_self"
	EnvelopeRescheduleReasonRateLimit   = "rate_limit"
)

// Envelope is a persistence-friendly snapshot of a queue job transition.
// It carries stable identifiers and timing metadata for adapters (for example Redis).
type Envelope struct {
	ID          string         // Queue-assigned job ID.
	Type        string         // Optional logical job type for replay (example: "send_invitation").
	Payload     []byte         // Optional serialized payload for replay.
	Attempt     int            // Current attempt number when reported.
	Status      EnvelopeStatus // Current status of the envelope.
	EnqueuedAt  time.Time      // Original enqueue timestamp.
	AvailableAt time.Time      // Time at which this instance was accepted by queue.
	ClaimedAt   time.Time      // Time at which a worker started processing.
	NextRunAt   time.Time      // Next desired execution time (optional, zero means immediate).
	LastError   string         // Last nack reason (optional).
}

// EnvelopeStore persists queue transitions in ack/nack form.
// Implementations can back this with Redis, SQL, files, etc.
type EnvelopeStore interface {
	// Enqueue records that an envelope was accepted by the queue.
	// This is called once per accepted enqueue before execution starts.
	Enqueue(ctx context.Context, env Envelope) error
	// Claim records that a worker started processing the envelope.
	// This is called right before executing the job function.
	Claim(ctx context.Context, id string) error
	// Ack records successful completion for the envelope.
	// This is called after the job returns nil.
	Ack(ctx context.Context, id string) error
	// Nack records failed completion for the envelope with the returned reason.
	// This is called after the job returns a non-nil error.
	Nack(ctx context.Context, id string, reason error) error
	// Recoverable returns envelopes that should be replayed now.
	// Implementations decide recovery policy (for example enqueued + stale claimed).
	Recoverable(ctx context.Context, now time.Time) ([]Envelope, error)
	// RecoverByID returns one envelope by its stable ID for targeted replay.
	RecoverByID(ctx context.Context, id string) (Envelope, error)
	// Reschedule records deferred execution for the envelope.
	// This is called when execution is intentionally delayed (for example release/rate-limit).
	Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error
	// SetPayload records replay metadata (logical type and payload bytes) for the envelope.
	SetPayload(ctx context.Context, id string, typ string, payload []byte) error
}

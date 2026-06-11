package cq

import (
	"context"
	"errors"
	"time"
)

// Reschedule reasons used by built-in wrappers.
const (
	RescheduleReasonRelease          = "release"
	RescheduleReasonReleaseSelf      = "release_self"
	RescheduleReasonRateLimit        = "rate_limit"
	RescheduleReasonManualRetry      = "manual_retry"
	RescheduleReasonConcurrencyLimit = "concurrency_limit"
)

// Attributes added to rescheduled submissions.
const (
	RescheduleAttributeParentID = "cq.reschedule.parent_id"
	RescheduleAttributeRootID   = "cq.reschedule.root_id"
	RescheduleAttributeReason   = "cq.reschedule.reason"
)

// Reschedule errors.
var (
	ErrRescheduleQueueRequired = errors.New("cq: reschedule queue required")
	ErrRescheduleJobRequired   = errors.New("cq: reschedule job required")
)

// Reschedule submits job after delay, preserving its name and attributes while
// recording lineage to the current execution. The returned handle tracks the
// new submission.
func Reschedule(ctx context.Context, queue *Queue, job Job, delay time.Duration, reason string) (*JobHandle, error) {
	if queue == nil {
		return nil, ErrRescheduleQueueRequired
	}
	if job == nil {
		return nil, ErrRescheduleJobRequired
	}
	if delay < 0 {
		delay = 0
	}
	if ctx == nil {
		ctx = context.Background()
	}

	meta := MetaFromContext(ctx)
	attributes := cloneStringMap(meta.Attributes)
	if attributes == nil {
		attributes = make(map[string]string)
	}
	if meta.ID != "" {
		if attributes[RescheduleAttributeRootID] == "" {
			attributes[RescheduleAttributeRootID] = meta.ID
		}
		attributes[RescheduleAttributeParentID] = meta.ID
	}
	if reason != "" {
		attributes[RescheduleAttributeReason] = reason
	}

	handle, err := queue.SubmitAfter(
		context.Background(),
		job,
		delay,
		WithJobName(meta.Name),
		WithJobAttributes(attributes),
	)
	if err != nil {
		return nil, err
	}
	queue.dispatchReschedule(meta, delay, reason)
	return handle, nil
}

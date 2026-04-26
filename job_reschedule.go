package cq

import (
	"context"
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

// Reschedule re-enqueues `job` after `delay` and emits an OnReschedule hook event.
// It returns false when queue/job are nil.
func Reschedule(ctx context.Context, queue *Queue, job Job, delay time.Duration, reason string) bool {
	if queue == nil || job == nil {
		return false
	}
	if delay < 0 {
		delay = 0
	}

	meta := MetaFromContext(ctx)
	queue.dispatchReschedule(meta, delay, reason)
	queue.DelayEnqueue(job, delay)
	return true
}

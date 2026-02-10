package cq

import (
	"context"
)

// JobState represents a job lifecycle state for queue tallies.
type JobState int

const (
	JobStateCreated JobState = iota
	JobStatePending
	JobStateActive
	JobStateFailed
	JobStateCompleted
)

// String implements fmt.Stringer.
func (js JobState) String() string {
	return [5]string{"created", "pending", "active", "failed", "completed"}[js]
}

// Job is the function signature processed by the queue.
type Job = func(ctx context.Context) error

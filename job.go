package cq

import (
	"context"
)

// JobState is the state of the job, used for queue tally lookups.
type JobState int

const (
	JobStateCreated JobState = iota
	JobStatePending
	JobStateActive
	JobStateFailed
	JobStateCompleted
)

// String() support for JobState.
func (js JobState) String() string {
	return [5]string{"created", "pending", "active", "failed", "completed"}[js]
}

// Job is type alias for the job signature.
type Job = func(ctx context.Context) error

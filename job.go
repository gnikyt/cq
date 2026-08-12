package cq

import (
	"context"
)

// JobState represents a job lifecycle state for queue tallies.
type JobState int

const (
	JobStateCreated   JobState = iota // Accepted by the queue.
	JobStatePending                   // Waiting for a worker.
	JobStateActive                    // Executing on a worker.
	JobStateFailed                    // Execution returned an error.
	JobStateCancelled                 // Cancelled through its handle.
	JobStateCompleted                 // Execution returned no error.
	JobStateDiscarded                 // Execution ended as a discarded outcome.
	JobStateAbandoned                 // Shutdown ended it before it ever started... no tally.
)

// String implements fmt.Stringer.
func (js JobState) String() string {
	return [8]string{
		"created", "pending", "active", "failed",
		"cancelled", "completed", "discarded", "abandoned",
	}[js]
}

// Job is the function signature processed by the queue.
type Job = func(ctx context.Context) error

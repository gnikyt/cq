package cq

import (
	"context"
	"testing"
)

// mustSubmit submits job or fails the current test.
func mustSubmit(t testing.TB, q *Queue, job Job, opts ...SubmitOption) *JobHandle {
	t.Helper()
	handle, err := q.Submit(context.Background(), job, opts...)
	if err != nil {
		t.Fatalf("Submit(): unexpected error: %v", err)
	}
	return handle
}

func TestJobStateString(t *testing.T) {
	tests := []struct {
		want  string
		state JobState
	}{
		{
			want:  "created",
			state: JobStateCreated,
		},
		{
			want:  "pending",
			state: JobStatePending,
		},
		{
			want:  "active",
			state: JobStateActive,
		},
		{
			want:  "failed",
			state: JobStateFailed,
		},
		{
			want:  "cancelled",
			state: JobStateCancelled,
		},
		{
			want:  "completed",
			state: JobStateCompleted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Errorf("JobState.String(): got %v, want %v", got, tt.want)
			}
		})
	}
}

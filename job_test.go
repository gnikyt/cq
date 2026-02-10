package cq

import (
	"testing"
)

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

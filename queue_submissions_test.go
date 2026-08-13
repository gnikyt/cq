package cq

import (
	"context"
	"testing"
	"time"
)

func TestSubmissionsReportsPendingAndActive(t *testing.T) {
	q := NewQueue(1, 1, 8)
	q.Start()
	defer q.Stop(false)

	release := make(chan struct{})
	started := make(chan struct{})
	running, err := q.Submit(context.Background(), func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	}, WithJobName("running"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-started

	// The single worker is busy, so this one stays buffered.
	waiting, err := q.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}, WithJobName("waiting"))
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}

	submissions := q.Submissions()
	if len(submissions) != 2 {
		t.Fatalf("Submissions(): got %d, want 2", len(submissions))
	}

	states := map[string]JobState{}
	for _, submission := range submissions {
		states[submission.Meta.ID] = submission.State
	}
	if got := states[running.ID()]; got != JobStateActive {
		t.Errorf("Submissions(): got %v for the running job, want JobStateActive", got)
	}
	if got := states[waiting.ID()]; got != JobStatePending {
		t.Errorf("Submissions(): got %v for the buffered job, want JobStatePending", got)
	}

	// Oldest enqueue first.
	if submissions[0].Meta.ID != running.ID() {
		t.Errorf("Submissions()[0].ID: got %q, want the older submission %q",
			submissions[0].Meta.ID, running.ID())
	}

	close(release)
	<-running.Done()
	<-waiting.Done()

	if remaining := q.Submissions(); len(remaining) != 0 {
		t.Errorf("Submissions() after completion: got %d, want 0", len(remaining))
	}
}

func TestSubmissionsIncludesDelayedAndMeta(t *testing.T) {
	q := NewQueue(1, 1, 8)
	q.Start()
	defer q.Stop(false)

	if _, err := q.SubmitAfter(context.Background(), func(ctx context.Context) error {
		return nil
	}, time.Hour, WithJobName("later"), WithJobAttribute("tenant", "acme")); err != nil {
		t.Fatalf("SubmitAfter(): %v", err)
	}

	submissions := q.Submissions()
	if len(submissions) != 1 {
		t.Fatalf("Submissions(): got %d, want 1", len(submissions))
	}
	if submissions[0].Meta.Name != "later" {
		t.Errorf("Submissions()[0].Meta.Name: got %q, want %q", submissions[0].Meta.Name, "later")
	}
	if submissions[0].Meta.Attributes["tenant"] != "acme" {
		t.Errorf("Submissions()[0] attributes: got %v, want tenant=acme", submissions[0].Meta.Attributes)
	}
	if submissions[0].State != JobStatePending {
		t.Errorf("Submissions()[0].State: got %v, want JobStatePending", submissions[0].State)
	}
}

// A handle stays tracked between finishing and being untracked. It must not
// be reported as in-flight during that window.
func TestSubmissionsOmitsTerminalHandles(t *testing.T) {
	q := NewQueue(1, 1, 8)
	q.Start()
	defer q.Stop(false)

	handle, err := q.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	<-handle.Done()

	// Simulate the window before untrack runs by re-tracking the finished handle.
	q.trackSubmission(handle)
	defer q.untrackSubmission(handle)

	if _, ok := handle.observedState(); ok {
		t.Error("observedState(): got ok=true for a finished handle, want false")
	}
	for _, submission := range q.Submissions() {
		if submission.Meta.ID == handle.ID() {
			t.Errorf("Submissions(): got the finished job %q, want it omitted", handle.ID())
		}
	}
}

func TestSubmissionsEmptyForIdleQueue(t *testing.T) {
	q := NewQueue(1, 1, 8)
	q.Start()
	defer q.Stop(false)

	if submissions := q.Submissions(); len(submissions) != 0 {
		t.Errorf("Submissions(): got %d, want 0", len(submissions))
	}
}

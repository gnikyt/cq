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

func TestPriorityQueueSubmissions(t *testing.T) {
	pq, base := newStalledPriorityQueue(t)
	defer pq.Stop(true)
	defer base.Stop(false)

	mustPrioritySubmit(t, pq, func(ctx context.Context) error { return nil }, PriorityHigh, WithJobName("a"))
	mustPrioritySubmit(t, pq, func(ctx context.Context) error { return nil }, PriorityLow, WithJobName("b"))

	subs := pq.Submissions()
	if len(subs) != 2 {
		t.Fatalf("Submissions(): got %d, want 2", len(subs))
	}
	for _, s := range subs {
		if s.State != JobStatePending {
			t.Errorf("Submissions(): got state %v, want pending", s.State)
		}
		if s.Meta.ID == "" || (s.Meta.Name != "a" && s.Meta.Name != "b") {
			t.Errorf("Submissions(): unexpected meta %+v", s.Meta)
		}
	}
}

func TestPriorityQueueSubmissionsEmpty(t *testing.T) {
	pq, base := newStalledPriorityQueue(t)
	defer pq.Stop(true)
	defer base.Stop(false)

	if got := pq.Submissions(); len(got) != 0 {
		t.Fatalf("Submissions(): got %d, want 0", len(got))
	}
}

func TestQueueManagerSubmissions(t *testing.T) {
	fast := NewQueue(0, 0, 16) // No workers... submissions stay pending.
	fast.Start()
	slow := NewQueue(0, 0, 16)
	slow.Start()

	mgr := NewQueueManager()
	if err := mgr.Register("fast", fast); err != nil {
		t.Fatalf("Register(fast): %v", err)
	}
	if err := mgr.Register("slow", slow); err != nil {
		t.Fatalf("Register(slow): %v", err)
	}
	defer mgr.StopAll(false)

	mustSubmit(t, fast, func(ctx context.Context) error { return nil })
	mustSubmit(t, slow, func(ctx context.Context) error { return nil })
	mustSubmit(t, slow, func(ctx context.Context) error { return nil })

	all := mgr.Submissions()
	if len(all) != 2 {
		t.Fatalf("Submissions(): got %d queues, want 2", len(all))
	}
	if got := len(all["fast"]); got != 1 {
		t.Errorf("Submissions()[fast]: got %d, want 1", got)
	}
	if got := len(all["slow"]); got != 2 {
		t.Errorf("Submissions()[slow]: got %d, want 2", got)
	}
}

func TestQueueManagerSubmissionsEmptyQueueMapsToEmptySlice(t *testing.T) {
	idle := NewQueue(1, 1, 16)
	idle.Start()
	mgr := NewQueueManager()
	if err := mgr.Register("idle", idle); err != nil {
		t.Fatalf("Register(idle): %v", err)
	}
	defer mgr.StopAll(false)

	all := mgr.Submissions()
	got, ok := all["idle"]
	if !ok {
		t.Fatal("Submissions(): idle queue missing from map")
	}
	if len(got) != 0 {
		t.Errorf("Submissions()[idle]: got %d, want 0", len(got))
	}
}

func TestPriorityQueueManagerSubmissions(t *testing.T) {
	pqa, basea := newStalledPriorityQueue(t)
	pqb, baseb := newStalledPriorityQueue(t)
	defer basea.Stop(false)
	defer baseb.Stop(false)

	mgr := NewPriorityQueueManager()
	if err := mgr.Register("a", pqa); err != nil {
		t.Fatalf("Register(a): %v", err)
	}
	if err := mgr.Register("b", pqb); err != nil {
		t.Fatalf("Register(b): %v", err)
	}
	defer mgr.StopAll(false)

	mustPrioritySubmit(t, pqa, func(ctx context.Context) error { return nil }, PriorityHigh)
	mustPrioritySubmit(t, pqb, func(ctx context.Context) error { return nil }, PriorityLow)
	mustPrioritySubmit(t, pqb, func(ctx context.Context) error { return nil }, PriorityMedium)

	all := mgr.Submissions()
	if len(all) != 2 {
		t.Fatalf("Submissions(): got %d queues, want 2", len(all))
	}
	if got := len(all["a"]); got != 1 {
		t.Errorf("Submissions()[a]: got %d, want 1", got)
	}
	if got := len(all["b"]); got != 2 {
		t.Errorf("Submissions()[b]: got %d, want 2", got)
	}
}

package cq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// abandonRecorder collects abandon hook events.
type abandonRecorder struct {
	mut    sync.Mutex
	events []JobEvent
}

func (r *abandonRecorder) hooks() Hooks {
	return Hooks{
		OnAbandon: func(_ context.Context, event JobEvent) {
			r.mut.Lock()
			defer r.mut.Unlock()
			r.events = append(r.events, event)
		},
	}
}

func (r *abandonRecorder) snapshot() []JobEvent {
	r.mut.Lock()
	defer r.mut.Unlock()
	return append([]JobEvent(nil), r.events...)
}

func TestStopDrainEmitsAbandonHook(t *testing.T) {
	rec := &abandonRecorder{}
	q := NewQueue(0, 0, 8, WithHooks(rec.hooks())) // No workers... nothing starts.
	q.Start()

	for range 3 {
		if _, err := q.Submit(context.Background(), func(ctx context.Context) error {
			return nil
		}, WithJobName("importer")); err != nil {
			t.Fatalf("Submit(): %v", err)
		}
	}

	drained, err := q.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("StopDrain(): got %v, want nil", err)
	}
	if len(drained) != 3 {
		t.Fatalf("StopDrain(): got %d drained jobs, want 3", len(drained))
	}

	events := rec.snapshot()
	if len(events) != 3 {
		t.Fatalf("OnAbandon: got %d events, want 3", len(events))
	}
	for _, event := range events {
		if event.State != JobStateAbandoned {
			t.Errorf("OnAbandon: got state %v, want JobStateAbandoned", event.State)
		}
		if !errors.Is(event.Err, ErrQueueDrained) {
			t.Errorf("OnAbandon: got err %v, want ErrQueueDrained", event.Err)
		}
		if event.Name != "importer" {
			t.Errorf("OnAbandon: got name %q, want %q", event.Name, "importer")
		}
		if event.ID == "" {
			t.Error("OnAbandon: got empty ID, want the submission ID")
		}
	}
}

func TestStopDrainEmitsAbandonHookForDelayed(t *testing.T) {
	rec := &abandonRecorder{}
	q := NewQueue(1, 1, 8, WithHooks(rec.hooks()))
	q.Start()

	if _, err := q.SubmitAfter(context.Background(), func(ctx context.Context) error {
		return nil
	}, time.Hour); err != nil {
		t.Fatalf("SubmitAfter(): %v", err)
	}

	drained, err := q.StopDrain(context.Background())
	if err != nil {
		t.Fatalf("StopDrain(): got %v, want nil", err)
	}
	if len(drained) != 1 {
		t.Fatalf("StopDrain(): got %d drained jobs, want 1", len(drained))
	}

	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("OnAbandon: got %d events, want 1", len(events))
	}
	if !errors.Is(events[0].Err, ErrQueueDrained) {
		t.Errorf("OnAbandon: got err %v, want ErrQueueDrained", events[0].Err)
	}
}

func TestStopWithoutWaitEmitsAbandonHook(t *testing.T) {
	rec := &abandonRecorder{}
	q := NewQueue(0, 0, 8, WithHooks(rec.hooks()))
	q.Start()

	for range 2 {
		if _, err := q.Submit(context.Background(), func(ctx context.Context) error {
			return nil
		}); err != nil {
			t.Fatalf("Submit(): %v", err)
		}
	}
	q.Stop(false)

	events := rec.snapshot()
	if len(events) != 2 {
		t.Fatalf("OnAbandon: got %d events, want 2", len(events))
	}
	for _, event := range events {
		if !errors.Is(event.Err, ErrJobAbandoned) {
			t.Errorf("OnAbandon: got err %v, want ErrJobAbandoned", event.Err)
		}
		if event.State != JobStateAbandoned {
			t.Errorf("OnAbandon: got state %v, want JobStateAbandoned", event.State)
		}
	}
}

func TestTerminateEmitsAbandonHook(t *testing.T) {
	rec := &abandonRecorder{}
	q := NewQueue(0, 0, 8, WithHooks(rec.hooks()))
	q.Start()

	if _, err := q.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	q.Terminate()

	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("OnAbandon: got %d events, want 1", len(events))
	}
	if !errors.Is(events[0].Err, ErrJobAbandoned) {
		t.Errorf("OnAbandon: got err %v, want ErrJobAbandoned", events[0].Err)
	}
}

func TestStopContextTimeoutEmitsAbandonHook(t *testing.T) {
	rec := &abandonRecorder{}
	q := NewQueue(1, 1, 8, WithHooks(rec.hooks()))
	q.Start()

	release := make(chan struct{})
	if _, err := q.Submit(context.Background(), func(ctx context.Context) error {
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	// Second job stays pending behind the blocked worker.
	if _, err := q.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("Submit(): %v", err)
	}

	err := q.StopTimeout(50 * time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopTimeout(): got %v, want context.DeadlineExceeded", err)
	}
	close(release)

	events := rec.snapshot()
	if len(events) == 0 {
		t.Fatal("OnAbandon: got 0 events, want at least 1 for the pending job")
	}
	for _, event := range events {
		if !errors.Is(event.Err, ErrJobAbandoned) {
			t.Errorf("OnAbandon: got err %v, want ErrJobAbandoned", event.Err)
		}
	}
}

// A hook that calls back into the queue must not deadlock: abandon events are
// dispatched after acceptMut is released.
func TestAbandonHookCanCallQueueWithoutDeadlock(t *testing.T) {
	var q *Queue
	done := make(chan error, 1)

	hooks := Hooks{
		OnAbandon: func(_ context.Context, event JobEvent) {
			// Submit on a stopped queue is rejected, but it still acquires
			// acceptMut... a hook dispatched under the lock would hang here.
			_, err := q.Submit(context.Background(), func(ctx context.Context) error {
				return nil
			})
			done <- err
		},
	}

	q = NewQueue(0, 0, 8, WithHooks(hooks))
	q.Start()
	if _, err := q.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("Submit(): %v", err)
	}

	finished := make(chan struct{})
	go func() {
		q.Stop(false)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop(false): deadlocked while dispatching the abandon hook")
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueStopped) {
			t.Errorf("Submit() from hook: got %v, want ErrQueueStopped", err)
		}
	case <-time.After(time.Second):
		t.Fatal("OnAbandon: hook never ran")
	}
}

func TestJobStateAbandonedString(t *testing.T) {
	if got := JobStateAbandoned.String(); got != "abandoned" {
		t.Errorf("JobStateAbandoned.String(): got %q, want %q", got, "abandoned")
	}
}

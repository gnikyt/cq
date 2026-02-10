package cq

import (
	"context"
	"testing"
	"time"
)

func TestJobRegistry(t *testing.T) {
	t.Run("basic_operations", func(t *testing.T) {
		registry := NewJobRegistry()

		// Create cancel functions.
		_, cancel1 := context.WithCancel(context.Background())
		_, cancel2 := context.WithCancel(context.Background())
		defer cancel1()
		defer cancel2()

		// Register jobs.
		registry.Register("job1", []string{"user:123", "email"}, cancel1)
		registry.Register("job2", []string{"user:123"}, cancel2)

		// Check counts.
		if count := registry.CountForTag("user:123"); count != 2 {
			t.Errorf("CountForTag(): got %d, want 2", count)
		}
		if count := registry.CountForTag("email"); count != 1 {
			t.Errorf("CountForTag(): got %d, want 1", count)
		}

		// Unregister one job.
		registry.Unregister("job1", []string{"user:123", "email"})

		if count := registry.CountForTag("user:123"); count != 1 {
			t.Errorf("CountForTag after unregister: got %d, want 1", count)
		}
		if count := registry.CountForTag("email"); count != 0 {
			t.Errorf("CountForTag for email after unregister: got %d, want 0", count)
		}
	})

	t.Run("cancel_by_tag", func(t *testing.T) {
		registry := NewJobRegistry()

		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel1()
		defer cancel2()

		registry.Register("job1", []string{"user:123"}, cancel1)
		registry.Register("job2", []string{"user:123"}, cancel2)

		// Cancel all jobs with tag.
		count := registry.CancelForTag("user:123")
		if count != 2 {
			t.Errorf("CancelForTag(): got %d, want 2", count)
		}

		// Contexts should be cancelled.
		select {
		case <-ctx1.Done():
			// Good.
		case <-time.After(100 * time.Millisecond):
			t.Error("ctx1 should have been cancelled")
		}

		select {
		case <-ctx2.Done():
			// Good.
		case <-time.After(100 * time.Millisecond):
			t.Error("ctx2 should have been cancelled")
		}
	})

	t.Run("cancel_all", func(t *testing.T) {
		registry := NewJobRegistry()

		ctx1, cancel1 := context.WithCancel(context.Background())
		_, cancel2 := context.WithCancel(context.Background())
		defer cancel1()
		defer cancel2()

		registry.Register("job1", []string{"tag1"}, cancel1)
		registry.Register("job2", []string{"tag2"}, cancel2)

		count := registry.CancelAll()
		if count != 2 {
			t.Errorf("CancelAll(): got %d, want 2", count)
		}

		// All contexts should be cancelled.
		select {
		case <-ctx1.Done():
			// Good.
		case <-time.After(100 * time.Millisecond):
			t.Error("ctx1 should have been cancelled")
		}
	})

	t.Run("tags_list", func(t *testing.T) {
		registry := NewJobRegistry()

		_, cancel1 := context.WithCancel(context.Background())
		_, cancel2 := context.WithCancel(context.Background())
		defer cancel1()
		defer cancel2()

		registry.Register("job1", []string{"tag1", "tag2"}, cancel1)
		registry.Register("job2", []string{"tag3"}, cancel2)

		tags := registry.Tags()
		if len(tags) != 3 {
			t.Errorf("Tags(): got %d tags, want 3", len(tags))
		}
	})

	t.Run("next_id", func(t *testing.T) {
		registry := NewJobRegistry()

		id1 := registry.NextID()
		id2 := registry.NextID()

		if id1 == id2 {
			t.Error("NextID() should generate unique IDs")
		}
	})
}

func TestWithTagged(t *testing.T) {
	t.Run("job_executes", func(t *testing.T) {
		registry := NewJobRegistry()
		var executed bool

		job := WithTagged(func(ctx context.Context) error {
			executed = true
			return nil
		}, registry, "test-tag")

		if err := job(context.Background()); err != nil {
			t.Errorf("WithTagged(): got %v, want nil", err)
		}

		if !executed {
			t.Error("WithTagged(): job should have executed")
		}

		// Job should have been unregistered after completion.
		if count := registry.CountForTag("test-tag"); count != 0 {
			t.Errorf("WithTagged(): tag count after completion: got %d, want 0", count)
		}
	})

	t.Run("cancel_by_tag", func(t *testing.T) {
		registry := NewJobRegistry()
		started := make(chan bool)
		cancelled := make(chan bool)

		job := WithTagged(func(ctx context.Context) error {
			started <- true
			<-ctx.Done()
			cancelled <- true
			return ctx.Err()
		}, registry, "cancel-test")

		go job(context.Background())

		// Wait for job to start.
		<-started

		// Cancel by tag.
		registry.CancelForTag("cancel-test")

		// Wait for cancellation.
		select {
		case <-cancelled:
			// Good.
		case <-time.After(100 * time.Millisecond):
			t.Error("WithTagged(): job should have been cancelled")
		}
	})

	t.Run("multiple_tags", func(t *testing.T) {
		registry := NewJobRegistry()

		job := WithTagged(func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		}, registry, "tag1", "tag2", "tag3")

		go job(context.Background())

		// Allow job to register.
		time.Sleep(5 * time.Millisecond)

		// All tags should have the job.
		if count := registry.CountForTag("tag1"); count != 1 {
			t.Errorf("WithTagged(): tag1 count: got %d, want 1", count)
		}
		if count := registry.CountForTag("tag2"); count != 1 {
			t.Errorf("WithTagged(): tag2 count: got %d, want 1", count)
		}
		if count := registry.CountForTag("tag3"); count != 1 {
			t.Errorf("WithTagged(): tag3 count: got %d, want 1", count)
		}
	})
}

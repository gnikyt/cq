package cq

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetaFromContext(t *testing.T) {
	t.Run("returns_zero_when_not_set", func(t *testing.T) {
		ctx := context.Background()
		meta := MetaFromContext(ctx)

		if meta.ID != "" {
			t.Errorf("MetaFromContext(): got ID %q, want empty", meta.ID)
		}
		if !meta.EnqueuedAt.IsZero() {
			t.Errorf("MetaFromContext(): got EnqueuedAt %v, want zero", meta.EnqueuedAt)
		}
		if meta.Attempt != 0 {
			t.Errorf("MetaFromContext(): got Attempt %d, want 0", meta.Attempt)
		}
	})

	t.Run("returns_meta_when_set", func(t *testing.T) {
		now := time.Now()
		meta := JobMeta{
			ID:         "42",
			EnqueuedAt: now,
			Attempt:    2,
		}
		ctx := contextWithMeta(context.Background(), meta)

		got := MetaFromContext(ctx)
		if got.ID != "42" {
			t.Errorf("MetaFromContext(): got ID %q, want %q", got.ID, "42")
		}
		if !got.EnqueuedAt.Equal(now) {
			t.Errorf("MetaFromContext(): got EnqueuedAt %v, want %v", got.EnqueuedAt, now)
		}
		if got.Attempt != 2 {
			t.Errorf("MetaFromContext(): got Attempt %d, want 2", got.Attempt)
		}
	})
}

func TestJobMetaInQueue(t *testing.T) {
	t.Run("job_receives_metadata", func(t *testing.T) {
		queue := NewQueue(1, 5, 10)
		queue.Start()
		defer queue.Stop(true)

		var receivedMeta JobMeta
		done := make(chan bool)

		queue.Enqueue(func(ctx context.Context) error {
			receivedMeta = MetaFromContext(ctx)
			done <- true
			return nil
		})

		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Fatal("job did not complete in time")
		}

		if receivedMeta.ID == "" {
			t.Error("JobMeta: ID should not be empty")
		}
		if receivedMeta.EnqueuedAt.IsZero() {
			t.Error("JobMeta: EnqueuedAt should not be zero")
		}
		if receivedMeta.Attempt != 0 {
			t.Errorf("JobMeta: got Attempt %d, want 0", receivedMeta.Attempt)
		}
	})

	t.Run("unique_ids_per_job", func(t *testing.T) {
		queue := NewQueue(1, 5, 10)
		queue.Start()
		defer queue.Stop(true)

		ids := make([]string, 3)
		var wg sync.WaitGroup
		wg.Add(3)

		for i := range 3 {
			i := i
			queue.Enqueue(func(ctx context.Context) error {
				ids[i] = MetaFromContext(ctx).ID
				wg.Done()
				return nil
			})
		}

		wg.Wait()

		seen := make(map[string]bool)
		for _, id := range ids {
			if seen[id] {
				t.Errorf("JobMeta: duplicate ID %q", id)
			}
			seen[id] = true
		}
	})

	t.Run("uses_custom_id_generator", func(t *testing.T) {
		queue := NewQueue(1, 5, 10, WithIDGenerator(func() string { return "custom-1" }))
		queue.Start()
		defer queue.Stop(true)

		done := make(chan JobMeta, 1)
		queue.Enqueue(func(ctx context.Context) error {
			done <- MetaFromContext(ctx)
			return nil
		})

		select {
		case meta := <-done:
			if meta.ID != "custom-1" {
				t.Fatalf("JobMeta: got ID=%q, want %q", meta.ID, "custom-1")
			}
		case <-time.After(1 * time.Second):
			t.Fatal("job did not complete in time")
		}
	})

}

func TestJobMetaWithRetry(t *testing.T) {
	t.Run("attempt_increments_on_retry", func(t *testing.T) {
		var attempts []int
		retryErr := errors.New("retry")

		job := WithRetry(func(ctx context.Context) error {
			meta := MetaFromContext(ctx)
			attempts = append(attempts, meta.Attempt)
			if len(attempts) < 3 {
				return retryErr
			}
			return nil
		}, 5)

		// Run directly (not through queue) to test retry logic.
		ctx := contextWithMeta(context.Background(), JobMeta{ID: "test", EnqueuedAt: time.Now()})
		if err := job(ctx); err != nil {
			t.Errorf("WithRetry(): got %v, want nil", err)
		}

		if len(attempts) != 3 {
			t.Fatalf("WithRetry(): got %d attempts, want 3", len(attempts))
		}
		for i, attempt := range attempts {
			if attempt != i {
				t.Errorf("WithRetry(): attempt %d: got Attempt %d, want %d", i, attempt, i)
			}
		}
	})

	t.Run("attempt_increments_through_queue", func(t *testing.T) {
		queue := NewQueue(1, 5, 10)
		queue.Start()
		defer queue.Stop(true)

		var lastAttempt atomic.Int32
		retryErr := errors.New("retry")
		done := make(chan bool)

		job := WithRetry(func(ctx context.Context) error {
			meta := MetaFromContext(ctx)
			lastAttempt.Store(int32(meta.Attempt))
			if meta.Attempt < 2 {
				return retryErr
			}
			done <- true
			return nil
		}, 5)

		queue.Enqueue(job)

		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Fatal("job did not complete in time")
		}

		if lastAttempt.Load() != 2 {
			t.Errorf("WithRetry(): got final Attempt %d, want 2", lastAttempt.Load())
		}
	})
}

func TestSetEnvelopePayload(t *testing.T) {
	t.Run("returns_false_without_setter", func(t *testing.T) {
		ok := SetEnvelopePayload(context.Background(), "email", []byte("payload"))
		if ok {
			t.Fatal("SetEnvelopePayload(): got true, want false")
		}
	})

	t.Run("writes_payload_with_setter", func(t *testing.T) {
		var (
			gotType    string
			gotPayload []byte
			calls      int
		)
		ctx := contextWithEnvelopePayloadSetter(context.Background(), func(typ string, payload []byte) bool {
			calls++
			gotType = typ
			gotPayload = append([]byte(nil), payload...)
			return true
		})

		ok := SetEnvelopePayload(ctx, "email", []byte("alpha"))
		if !ok {
			t.Fatal("SetEnvelopePayload(): got false, want true")
		}
		if calls != 1 {
			t.Fatalf("SetEnvelopePayload(): got calls=%d, want 1", calls)
		}
		if gotType != "email" {
			t.Fatalf("SetEnvelopePayload(): got type=%q, want %q", gotType, "email")
		}
		if !bytes.Equal(gotPayload, []byte("alpha")) {
			t.Fatalf("SetEnvelopePayload(): got payload=%q, want %q", string(gotPayload), "alpha")
		}
	})

	t.Run("last_write_wins", func(t *testing.T) {
		var (
			gotType    string
			gotPayload []byte
			calls      int
		)
		ctx := contextWithEnvelopePayloadSetter(context.Background(), func(typ string, payload []byte) bool {
			calls++
			gotType = typ
			gotPayload = append([]byte(nil), payload...)
			return true
		})

		_ = SetEnvelopePayload(ctx, "email", []byte("alpha"))
		_ = SetEnvelopePayload(ctx, "email.v2", []byte("beta"))

		if calls != 2 {
			t.Fatalf("SetEnvelopePayload(): got calls=%d, want 2", calls)
		}
		if gotType != "email.v2" {
			t.Fatalf("SetEnvelopePayload(): got type=%q, want %q", gotType, "email.v2")
		}
		if !bytes.Equal(gotPayload, []byte("beta")) {
			t.Fatalf("SetEnvelopePayload(): got payload=%q, want %q", string(gotPayload), "beta")
		}
	})
}

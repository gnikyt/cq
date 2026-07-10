package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithExpiryFreshJobRuns(t *testing.T) {
	ran := false
	job := WithExpiry(func(ctx context.Context) error {
		ran = true
		return nil
	}, time.Minute)

	ctx := contextWithMeta(context.Background(), JobMeta{ID: "1", EnqueuedAt: time.Now()})
	if err := job(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ran {
		t.Fatal("expected fresh job to run")
	}
}

func TestWithExpiryStaleJobDiscarded(t *testing.T) {
	ran := false
	job := WithExpiry(func(ctx context.Context) error {
		ran = true
		return nil
	}, time.Millisecond)

	ctx := contextWithMeta(context.Background(), JobMeta{ID: "1", EnqueuedAt: time.Now().Add(-time.Second)})
	err := job(ctx)
	if ran {
		t.Fatal("expected stale job to be skipped")
	}
	if !errors.Is(err, ErrJobExpired) {
		t.Errorf("got %v, want ErrJobExpired", err)
	}
	if !errors.Is(err, ErrDiscard) {
		t.Errorf("got %v, want ErrDiscard outcome", err)
	}
}

func TestWithExpiryWithoutMetaRuns(t *testing.T) {
	ran := false
	job := WithExpiry(func(ctx context.Context) error {
		ran = true
		return nil
	}, time.Millisecond)

	if err := job(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ran {
		t.Fatal("expected job without enqueue time to run")
	}
}

func TestWithExpiryNonPositiveTTLRuns(t *testing.T) {
	ran := false
	job := WithExpiry(func(ctx context.Context) error {
		ran = true
		return nil
	}, 0)

	ctx := contextWithMeta(context.Background(), JobMeta{ID: "1", EnqueuedAt: time.Now().Add(-time.Hour)})
	if err := job(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ran {
		t.Fatal("expected job with zero ttl to run")
	}
}

func TestWithExpiryOnQueue(t *testing.T) {
	queue := NewQueue(1, 2, 16)
	queue.Start()
	defer queue.Stop(true)

	// Pause so the job waits in the buffer past its expiry.
	if err := queue.Pause(); err != nil {
		t.Fatalf("pause: %v", err)
	}

	ran := false
	job := WithExpiry(func(ctx context.Context) error {
		ran = true
		return nil
	}, 20*time.Millisecond)

	handle, err := queue.Submit(context.Background(), job)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if err := queue.Resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}

	err = handle.Wait(context.Background())
	if !errors.Is(err, ErrJobExpired) || !errors.Is(err, ErrDiscard) {
		t.Fatalf("Wait(): got %v, want expired discard outcome", err)
	}
	if ran {
		t.Fatal("expected expired job not to run")
	}

	queue.Stop(true)
	if got := queue.TallyOf(JobStateDiscarded); got != 1 {
		t.Errorf("TallyOf(JobStateDiscarded): got %d, want 1", got)
	}
	if got := queue.TallyOf(JobStateFailed); got != 0 {
		t.Errorf("TallyOf(JobStateFailed): got %d, want 0", got)
	}
}

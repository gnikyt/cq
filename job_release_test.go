package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWithRelease(t *testing.T) {
	t.Run("release_on_error", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		releaseErr := errors.New("release me")

		job := WithRelease(
			func(ctx context.Context) error {
				if calls.Add(1) < 2 {
					return releaseErr
				}
				return nil
			},
			queue,
			10*time.Millisecond,
			2,
			func(err error) bool {
				return errors.Is(err, releaseErr)
			},
		)

		// First call... should release.
		if err := job(context.Background()); err != nil {
			t.Errorf("WithRelease(): got %v, want nil (on release)", err)
		}

		// Wait for re-enqueue.
		time.Sleep(20 * time.Millisecond)

		// Should have been called twice (initial + 1 release).
		if got := calls.Load(); got < 2 {
			t.Errorf("WithRelease(): calls: got %d, want >= 2", got)
		}
	})

	t.Run("max_releases_exceeded", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		releaseErr := errors.New("release me")
		maxReleases := 2

		job := WithRelease(
			func(ctx context.Context) error {
				calls.Add(1)
				return releaseErr
			},
			queue,
			10*time.Millisecond,
			maxReleases,
			func(err error) bool {
				return errors.Is(err, releaseErr)
			},
		)

		// Call until max releases.
		for i := 0; i <= maxReleases; i++ {
			err := job(context.Background())
			if i < maxReleases {
				if err != nil {
					t.Errorf("WithRelease(): release %d: got %v, want nil", i, err)
				}
			} else {
				// Should return error after max releases.
				if err == nil {
					t.Error("WithRelease(): should return error after max releases")
				}
			}
		}
	})

	t.Run("no_release_on_different_error", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		releaseErr := errors.New("release me")
		otherErr := errors.New("other error")

		job := WithRelease(
			func(ctx context.Context) error {
				return otherErr
			},
			queue,
			10*time.Millisecond,
			2,
			func(err error) bool {
				return errors.Is(err, releaseErr)
			},
		)

		// Should return error, not release.
		err := job(context.Background())
		if err == nil {
			t.Error("WithRelease(): should return error for non-release error")
		}
		if !errors.Is(err, otherErr) {
			t.Errorf("WithRelease(): got error %v, want %v", err, otherErr)
		}
	})

	t.Run("release_counter_persists_through_queue", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls int
		var mu sync.Mutex
		releaseErr := errors.New("release me")
		maxReleases := 2

		job := WithRelease(
			func(ctx context.Context) error {
				mu.Lock()
				calls++
				mu.Unlock()
				return releaseErr // Always fails to test max releases.
			},
			queue,
			20*time.Millisecond,
			maxReleases,
			func(err error) bool {
				return errors.Is(err, releaseErr)
			},
		)

		// Enqueue the wrapped job (not calling directly).
		queue.Enqueue(job)

		// Wait for all releases to process through the queue.
		// Should be: initial call + 2 releases = 3 total calls.
		time.Sleep(150 * time.Millisecond)

		mu.Lock()
		totalCalls := calls
		mu.Unlock()
		expectedCalls := maxReleases + 1 // Initial + max releases.
		if totalCalls != expectedCalls {
			t.Errorf("WithRelease(): calls through queue: got %d, want %d", totalCalls, expectedCalls)
		}

		// Final tally should show 1 failed job (after max releases exceeded).
		failed := queue.TallyOf(JobStateFailed)
		if failed != 1 {
			t.Errorf("WithRelease(): failed jobs: got %d, want 1", failed)
		}
	})
}

func TestWithReleaseSelf(t *testing.T) {
	t.Run("requests_release_and_reenqueues", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		done := make(chan struct{}, 1)

		job := WithReleaseSelf(func(ctx context.Context) error {
			n := calls.Add(1)
			if n == 1 {
				if ok := RequestRelease(ctx, 10*time.Millisecond); !ok {
					t.Error("RequestRelease(): expected true")
				}
				return nil // First run, return nil to indicate release request was successful.
			}

			// Second run, signal completion. This tells us it was re-enqueued.
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		}, queue, 1)

		queue.Enqueue(job)

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("WithReleaseSelf(): timed out waiting for re-enqueued run, calls=%d", calls.Load())
		}
	})

	t.Run("release_wins_over_error", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		done := make(chan struct{}, 1)

		job := WithReleaseSelf(func(ctx context.Context) error {
			n := calls.Add(1)
			if n == 1 {
				_ = RequestRelease(ctx, 10*time.Millisecond)
				return errors.New("transient")
			}

			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		}, queue, 1)

		// Direct call with metadata to assert release wins (returns nil).
		if err := job(contextWithMeta(context.Background(), JobMeta{ID: "self-release-win"})); err != nil {
			t.Fatalf("WithReleaseSelf(): got %v, want nil when release requested", err)
		}

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("WithReleaseSelf(): timed out waiting for release re-enqueue, calls=%d", calls.Load())
		}
	})

	t.Run("max_releases_cap", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		job := WithReleaseSelf(func(ctx context.Context) error {
			calls.Add(1)
			_ = RequestRelease(ctx, 10*time.Millisecond)
			return nil
		}, queue, 1) // Only one release allowed.

		queue.Enqueue(job)
		time.Sleep(120 * time.Millisecond)

		if got := calls.Load(); got != 2 {
			t.Fatalf("WithReleaseSelf(): calls: got %d, want 2 (initial + 1 release)", got)
		}
	})

	t.Run("request_without_release_self_returns_false", func(t *testing.T) {
		if ok := RequestRelease(context.Background(), 10*time.Millisecond); ok {
			t.Fatal("RequestRelease(): got true, want false without wrapper context")
		}
	})

	t.Run("last_write_wins_for_delay", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		done := make(chan struct{}, 1)

		job := WithReleaseSelf(func(ctx context.Context) error {
			n := calls.Add(1)
			if n == 1 {
				_ = RequestRelease(ctx, 80*time.Millisecond)
				_ = RequestRelease(ctx, 5*time.Millisecond) // Last write should win.
				return nil
			}
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		}, queue, 1)

		queue.Enqueue(job)

		select {
		case <-done:
			// Should arrive quickly if 5ms won.
		case <-time.After(40 * time.Millisecond):
			t.Fatal("WithReleaseSelf(): expected second run before 40ms (last write wins)")
		}
	})

	t.Run("negative_max_releases_treated_as_unlimited", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		done := make(chan struct{}, 1)

		job := WithReleaseSelf(func(ctx context.Context) error {
			n := calls.Add(1)
			if n <= 3 {
				_ = RequestRelease(ctx, 5*time.Millisecond)
			}
			if n == 4 {
				select {
				case done <- struct{}{}:
				default:
				}
			}
			return nil
		}, queue, -1) // Should behave as unlimited (0).

		queue.Enqueue(job)
		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
			t.Fatalf("WithReleaseSelf(): expected repeated releases with negative max, calls=%d", calls.Load())
		}
	})

	t.Run("panic_after_request_does_not_leak_state", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		job := WithReleaseSelf(func(ctx context.Context) error {
			n := calls.Add(1)
			if n == 1 {
				_ = RequestRelease(ctx, 5*time.Millisecond)
				panic("boom")
			}
			return nil
		}, queue, 1)

		func() {
			defer func() { _ = recover() }()
			_ = job(contextWithMeta(context.Background(), JobMeta{ID: "panic-cleanup-id"}))
		}()

		// Same ID: should not inherit stale request from panic run.
		if err := job(contextWithMeta(context.Background(), JobMeta{ID: "panic-cleanup-id"})); err != nil {
			t.Fatalf("WithReleaseSelf(): second call got %v, want nil", err)
		}
		time.Sleep(30 * time.Millisecond)
		if got := calls.Load(); got != 2 {
			t.Fatalf("WithReleaseSelf(): stale request leaked after panic, calls=%d want 2", got)
		}
	})

	t.Run("late_async_request_is_rejected", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls atomic.Int32
		asyncResult := make(chan bool, 1)

		job := WithReleaseSelf(func(ctx context.Context) error {
			n := calls.Add(1)
			if n == 1 {
				go func(c context.Context) {
					time.Sleep(20 * time.Millisecond) // After run has ended.
					ok := RequestRelease(c, 5*time.Millisecond)
					asyncResult <- ok
				}(ctx)
			}
			return nil
		}, queue, 1)

		// First run schedules async late RequestRelease.
		if err := job(contextWithMeta(context.Background(), JobMeta{ID: "late-async-id"})); err != nil {
			t.Fatalf("WithReleaseSelf(): first call got %v, want nil", err)
		}

		// Wait for async attempt... should be rejected.
		select {
		case ok := <-asyncResult:
			if ok {
				t.Fatal("RequestRelease(): late async call should be rejected")
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatal("WithReleaseSelf(): timed out waiting for async request result")
		}

		// Second run with same ID should not consume stale request.
		if err := job(contextWithMeta(context.Background(), JobMeta{ID: "late-async-id"})); err != nil {
			t.Fatalf("WithReleaseSelf(): second call got %v, want nil", err)
		}
		time.Sleep(40 * time.Millisecond)
		if got := calls.Load(); got != 2 {
			t.Fatalf("WithReleaseSelf(): stale async request affected later run, calls=%d want 2", got)
		}
	})
}

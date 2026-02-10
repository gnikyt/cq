package cq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWithRelease(t *testing.T) {
	t.Run("release_on_error", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls int
		releaseErr := errors.New("release me")

		job := WithRelease(
			func(ctx context.Context) error {
				calls++
				if calls < 2 {
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
		if calls < 2 {
			t.Errorf("WithRelease(): calls: got %d, want >= 2", calls)
		}
	})

	t.Run("max_releases_exceeded", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		var calls int
		releaseErr := errors.New("release me")
		maxReleases := 2

		job := WithRelease(
			func(ctx context.Context) error {
				calls++
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

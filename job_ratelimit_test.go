package cq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestWithRateLimit(t *testing.T) {
	t.Run("limits_rate", func(t *testing.T) {
		// Allow 2 jobs per second with burst of 1.
		limiter := rate.NewLimiter(2, 1)

		var timestamps []time.Time
		var mu sync.Mutex

		job := WithRateLimit(func(ctx context.Context) error {
			mu.Lock()
			timestamps = append(timestamps, time.Now())
			mu.Unlock()
			return nil
		}, limiter)

		// Run 3 jobs sequentially.
		for range 3 {
			if err := job(context.Background()); err != nil {
				t.Errorf("WithRateLimit(): got %v, want nil", err)
			}
		}

		mu.Lock()
		defer mu.Unlock()

		if len(timestamps) != 3 {
			t.Fatalf("WithRateLimit(): got %d executions, want 3", len(timestamps))
		}

		// First job should execute immediately (burst).
		// Second and third should be rate limited (~500ms apart at 2/sec).
		gap := timestamps[2].Sub(timestamps[0])
		if gap < 400*time.Millisecond {
			t.Errorf("WithRateLimit(): got gap %v, want >= 400ms (rate limited)", gap)
		}
	})

	t.Run("context_cancellation", func(t *testing.T) {
		// Very slow rate: 1 per 10 seconds, no burst.
		limiter := rate.NewLimiter(rate.Every(10*time.Second), 1)

		// Consume the burst token.
		limiter.Allow()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		job := WithRateLimit(func(ctx context.Context) error {
			t.Error("WithRateLimit(): job should not execute")
			return nil
		}, limiter)

		err := job(ctx)
		if err == nil {
			t.Error("WithRateLimit(): got nil, want error")
		}
	})

	t.Run("burst_allows_immediate", func(t *testing.T) {
		// Allow burst of 5.
		limiter := rate.NewLimiter(1, 5)

		var count atomic.Int32
		start := time.Now()

		job := WithRateLimit(func(ctx context.Context) error {
			count.Add(1)
			return nil
		}, limiter)

		// Run 5 jobs... should all execute immediately due to burst.
		for range 5 {
			if err := job(context.Background()); err != nil {
				t.Errorf("WithRateLimit(): got %v, want nil", err)
			}
		}

		elapsed := time.Since(start)
		if elapsed > 100*time.Millisecond {
			t.Errorf("WithRateLimit(): burst took %v, want < 100ms", elapsed)
		}
		if count.Load() != 5 {
			t.Errorf("WithRateLimit(): got %d executions, want 5", count.Load())
		}
	})

	t.Run("with_queue", func(t *testing.T) {
		queue := NewQueue(5, 10, 100)
		queue.Start()
		defer queue.Stop(true)

		// Allow 10 jobs per second with burst of 2.
		limiter := rate.NewLimiter(10, 2)

		var count atomic.Int32
		var wg sync.WaitGroup

		for range 5 {
			wg.Add(1)
			job := WithRateLimit(func(ctx context.Context) error {
				count.Add(1)
				wg.Done()
				return nil
			}, limiter)
			queue.Enqueue(job)
		}

		wg.Wait()

		if count.Load() != 5 {
			t.Errorf("WithRateLimit(): got %d executions, want 5", count.Load())
		}
	})
}

func TestWithRateLimitRelease(t *testing.T) {
	t.Run("releases_when_limited", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		// Slow limiter: 1 token every 200ms, burst 1.
		limiter := rate.NewLimiter(rate.Every(200*time.Millisecond), 1)
		_ = limiter.Allow() // consume burst

		var count atomic.Int32
		done := make(chan struct{}, 1)

		job := WithRateLimitRelease(func(ctx context.Context) error {
			count.Add(1)
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		}, limiter, queue, 2)

		start := time.Now()
		if err := job(context.Background()); err != nil {
			t.Fatalf("WithRateLimitRelease(): got %v, want nil on release", err)
		}
		// Should return quickly because it releases instead of waiting.
		if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
			t.Fatalf("WithRateLimitRelease(): elapsed %v, want fast return", elapsed)
		}

		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
			t.Fatal("WithRateLimitRelease(): expected re-enqueued execution")
		}
		if got := count.Load(); got != 1 {
			t.Fatalf("WithRateLimitRelease(): count=%d, want 1", got)
		}
	})

	t.Run("max_releases_exhausted_falls_back_to_wait", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		limiter := rate.NewLimiter(rate.Every(120*time.Millisecond), 1)
		_ = limiter.Allow() // consume burst

		var count atomic.Int32
		job := WithRateLimitRelease(func(ctx context.Context) error {
			count.Add(1)
			return nil
		}, limiter, queue, 1)

		// First call: released.
		if err := job(context.Background()); err != nil {
			t.Fatalf("WithRateLimitRelease(): first call got %v, want nil", err)
		}

		// Immediate second call: release budget exhausted, so should block+run.
		start := time.Now()
		if err := job(context.Background()); err != nil {
			t.Fatalf("WithRateLimitRelease(): second call got %v, want nil", err)
		}
		if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
			t.Fatalf("WithRateLimitRelease(): expected blocking fallback, elapsed=%v", elapsed)
		}

		if got := count.Load(); got < 1 {
			t.Fatalf("WithRateLimitRelease(): count=%d, want >=1", got)
		}
	})

	t.Run("negative_max_releases_treated_as_unlimited", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		limiter := rate.NewLimiter(rate.Every(80*time.Millisecond), 1)
		_ = limiter.Allow() // consume burst

		var count atomic.Int32
		done := make(chan struct{}, 1)
		job := WithRateLimitRelease(func(ctx context.Context) error {
			if count.Add(1) >= 1 {
				select {
				case done <- struct{}{}:
				default:
				}
			}
			return nil
		}, limiter, queue, -1)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithRateLimitRelease(): got %v, want nil", err)
		}
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("WithRateLimitRelease(): expected release with negative maxReleases")
		}
	})
}

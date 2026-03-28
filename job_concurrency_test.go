package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWithConcurrencyLimit(t *testing.T) {
	t.Run("executes_when_below_limit", func(t *testing.T) {
		queue := NewQueue(2, 5, 50)
		queue.Start()
		defer queue.Stop(true)

		limiter := NewConcurrencyLimiter()
		var count atomic.Int32
		var wg sync.WaitGroup

		for range 3 {
			wg.Add(1)
			job := WithConcurrencyLimit(func(ctx context.Context) error {
				count.Add(1)
				wg.Done()
				return nil
			}, "key", 5, 0, limiter, queue)
			queue.Enqueue(job)
		}

		wg.Wait()

		if got := count.Load(); got != 3 {
			t.Errorf("WithConcurrencyLimit(): got %d executions, want 3", got)
		}
	})

	t.Run("re_enqueues_when_at_limit", func(t *testing.T) {
		queue := NewQueue(2, 10, 50)
		queue.Start()
		defer queue.Stop(true)

		limiter := NewConcurrencyLimiter()
		max := 2

		var count atomic.Int32
		var wg sync.WaitGroup
		barrier := make(chan struct{})

		// Enqueue max jobs that hold at the barrier.
		for range max {
			wg.Add(1)
			job := WithConcurrencyLimit(func(ctx context.Context) error {
				<-barrier
				count.Add(1)
				wg.Done()
				return nil
			}, "key", max, 30*time.Millisecond, limiter, queue)
			queue.Enqueue(job)
		}

		// Give barrier-holding jobs time to start.
		time.Sleep(50 * time.Millisecond)

		// Enqueue one more — should be at limit and get re-enqueued.
		wg.Add(1)
		extra := WithConcurrencyLimit(func(ctx context.Context) error {
			count.Add(1)
			wg.Done()
			return nil
		}, "key", max, 30*time.Millisecond, limiter, queue)
		queue.Enqueue(extra)

		// Release the barrier so all jobs can finish.
		close(barrier)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("WithConcurrencyLimit(): timed out waiting for all jobs to complete")
		}

		if got := count.Load(); got != int32(max+1) {
			t.Errorf("WithConcurrencyLimit(): got %d executions, want %d", got, max+1)
		}
	})

	t.Run("releases_slot_on_success", func(t *testing.T) {
		limiter := NewConcurrencyLimiter()
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		job := WithConcurrencyLimit(func(ctx context.Context) error {
			return nil
		}, "key", 1, 0, limiter, queue)

		if err := job(context.Background()); err != nil {
			t.Fatalf("WithConcurrencyLimit(): got %v, want nil", err)
		}
		if got := limiter.ActiveFor("key"); got != 0 {
			t.Errorf("WithConcurrencyLimit(): ActiveFor got %d, want 0", got)
		}
	})

	t.Run("releases_slot_on_error", func(t *testing.T) {
		limiter := NewConcurrencyLimiter()
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		jobErr := errors.New("inner failure")
		job := WithConcurrencyLimit(func(ctx context.Context) error {
			return jobErr
		}, "key", 1, 0, limiter, queue)

		err := job(context.Background())
		if !errors.Is(err, jobErr) {
			t.Fatalf("WithConcurrencyLimit(): got %v, want %v", err, jobErr)
		}
		if got := limiter.ActiveFor("key"); got != 0 {
			t.Errorf("WithConcurrencyLimit(): ActiveFor got %d, want 0", got)
		}
	})

	t.Run("max_concurrent_never_exceeded", func(t *testing.T) {
		queue := NewQueue(5, 10, 100)
		queue.Start()
		defer queue.Stop(true)

		limiter := NewConcurrencyLimiter()
		max := 3
		total := 20

		var peak atomic.Int32
		var wg sync.WaitGroup

		for range total {
			wg.Add(1)
			job := WithConcurrencyLimit(func(ctx context.Context) error {
				for {
					cur := peak.Load()
					active := int32(limiter.ActiveFor("key"))
					if active <= cur {
						break
					}
					if peak.CompareAndSwap(cur, active) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				wg.Done()
				return nil
			}, "key", max, 20*time.Millisecond, limiter, queue)
			queue.Enqueue(job)
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("WithConcurrencyLimit(): timed out")
		}

		if got := peak.Load(); got > int32(max) {
			t.Errorf("WithConcurrencyLimit(): peak %d exceeded max %d", got, max)
		}
	})

	t.Run("zero_retry_delay_uses_default", func(t *testing.T) {
		queue := NewQueue(1, 2, 10)
		queue.Start()
		defer queue.Stop(true)

		limiter := NewConcurrencyLimiter()
		done := make(chan struct{}, 1)

		// Saturate the limiter.
		holding := make(chan struct{})
		holderJob := WithConcurrencyLimit(func(ctx context.Context) error {
			<-holding
			return nil
		}, "key", 1, 0, limiter, queue)
		queue.Enqueue(holderJob)

		time.Sleep(30 * time.Millisecond)

		// This one should re-enqueue without panicking (retryDelay=0 defaults to 50ms).
		secondJob := WithConcurrencyLimit(func(ctx context.Context) error {
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		}, "key", 1, 0, limiter, queue)
		queue.Enqueue(secondJob)

		// Release the holder.
		close(holding)

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("WithConcurrencyLimit(): timed out waiting for re-enqueued job")
		}
	})
}

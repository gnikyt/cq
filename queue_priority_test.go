package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPriorityString(t *testing.T) {
	tests := []struct {
		want     string
		priority Priority
	}{
		{
			want:     "LOWEST",
			priority: PriorityLowest,
		},
		{
			want:     "LOW",
			priority: PriorityLow,
		},
		{
			want:     "MEDIUM",
			priority: PriorityMedium,
		},
		{
			want:     "HIGH",
			priority: PriorityHigh,
		},
		{
			want:     "HIGHEST",
			priority: PriorityHighest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.priority.String(); got != tt.want {
				t.Errorf("Priority.String(): got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPriorityQueue(t *testing.T) {
	t.Run("basic_enqueue", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		var executed atomic.Bool

		job := func(ctx context.Context) error {
			executed.Store(true)
			return nil
		}

		mustPrioritySubmit(t, pq, job, PriorityHigh)

		// Wait for job to process.
		time.Sleep(50 * time.Millisecond)

		if !executed.Load() {
			t.Error("PriorityQueue: job should have executed")
		}
	})

	t.Run("priority_ordering", func(t *testing.T) {
		queue := NewQueue(1, 1, 100) // Single worker to ensure ordering.
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		var order []int
		var mut sync.Mutex

		makeJob := func(id int) Job {
			return func(ctx context.Context) error {
				mut.Lock()
				order = append(order, id)
				mut.Unlock()
				return nil
			}
		}

		// Submit in reverse priority order.
		mustPrioritySubmit(t, pq, makeJob(5), PriorityLowest)
		mustPrioritySubmit(t, pq, makeJob(4), PriorityLow)
		mustPrioritySubmit(t, pq, makeJob(3), PriorityMedium)
		mustPrioritySubmit(t, pq, makeJob(2), PriorityHigh)
		mustPrioritySubmit(t, pq, makeJob(1), PriorityHighest)

		// Wait for all jobs to process.
		time.Sleep(100 * time.Millisecond)

		mut.Lock()
		defer mut.Unlock()
		if len(order) != 5 {
			t.Fatalf("PriorityQueue: got %d jobs, want 5", len(order))
		}

		// Highest priority should execute first.
		if order[0] != 1 {
			t.Errorf("PriorityQueue: first job: got %d, want 1 (highest)", order[0])
		}
	})

	t.Run("stop_with_queue", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()

		pq := NewPriorityQueue(queue, 10)

		// Stop with stopQueue = true.
		pq.Stop(true)

		// Queue should be stopped.
		if !queue.IsStopped() {
			t.Error("PriorityQueue.Stop(true): queue should be stopped")
		}
	})

	t.Run("stop_without_queue", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)

		// Stop with stopQueue = false.
		pq.Stop(false)

		// Queue should still be running.
		if queue.IsStopped() {
			t.Error("PriorityQueue.Stop(false): queue should still be running")
		}
	})

	t.Run("weighted_number", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10,
			WithWeighting(
				NumberWeight(10),
				NumberWeight(5),
				NumberWeight(3),
				NumberWeight(2),
				NumberWeight(1),
			),
		)
		defer pq.Stop(false)

		if pq.weights.highest != 10 {
			t.Errorf("WithWeighting: highest weight: got %d, want 10", pq.weights.highest)
		}
		if pq.weights.high != 5 {
			t.Errorf("WithWeighting: high weight: got %d, want 5", pq.weights.high)
		}
	})

	t.Run("weighted_percent", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10,
			WithWeighting(
				PercentWeight(50), // 50% of 12 = 6
				PercentWeight(25), // 25% of 12 = 3
				PercentWeight(17), // 17% of 12 = 2
				PercentWeight(8),  // 8% of 12 = 0 -> 1 (min)
				PercentWeight(8),  // 8% of 12 = 0 -> 1 (min)
			),
		)
		defer pq.Stop(false)

		if pq.weights.highest != 6 {
			t.Errorf("WithWeighting percent: highest weight: got %d, want 6", pq.weights.highest)
		}
		if pq.weights.high != 3 {
			t.Errorf("WithWeighting percent: high weight: got %d, want 3", pq.weights.high)
		}
		if pq.weights.medium != 2 {
			t.Errorf("WithWeighting percent: medium weight: got %d, want 2", pq.weights.medium)
		}
		// Low and lowest should be at least 1 (floor)
		if pq.weights.low < 1 {
			t.Errorf("WithWeighting percent: low weight: got %d, want >= 1", pq.weights.low)
		}
		if pq.weights.lowest < 1 {
			t.Errorf("WithWeighting percent: lowest weight: got %d, want >= 1", pq.weights.lowest)
		}
	})

	t.Run("default_weights", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		// Should use default weights (5:3:2:1:1)
		if pq.weights.highest != defaultWeightHighest {
			t.Errorf("Default weights: highest: got %d, want %d", pq.weights.highest, defaultWeightHighest)
		}
		if pq.weights.high != defaultWeightHigh {
			t.Errorf("Default weights: high: got %d, want %d", pq.weights.high, defaultWeightHigh)
		}
		if pq.weights.medium != defaultWeightMedium {
			t.Errorf("Default weights: medium: got %d, want %d", pq.weights.medium, defaultWeightMedium)
		}
		if pq.weights.low != defaultWeightLow {
			t.Errorf("Default weights: low: got %d, want %d", pq.weights.low, defaultWeightLow)
		}
		if pq.weights.lowest != defaultWeightLowest {
			t.Errorf("Default weights: lowest: got %d, want %d", pq.weights.lowest, defaultWeightLowest)
		}
	})

	t.Run("try_enqueue_success", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		var executed atomic.Bool
		job := func(ctx context.Context) error {
			executed.Store(true)
			return nil
		}

		// Should succeed as channel has capacity.
		if _, err := pq.Submit(context.Background(), job, PriorityHigh, WithNonBlocking()); err != nil {
			t.Fatalf("Submit(non-blocking): unexpected err: %v", err)
		}

		// Wait for job to process.
		time.Sleep(50 * time.Millisecond)

		if !executed.Load() {
			t.Error("Submit(non-blocking): job should have executed")
		}
	})

	t.Run("enqueue_or_error_invalid_falls_back_medium", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		var executed atomic.Bool
		job := func(ctx context.Context) error {
			executed.Store(true)
			return nil
		}

		if handle, err := pq.Submit(context.Background(), job, Priority(999)); handle != nil || !errors.Is(err, ErrPriorityInvalid) {
			t.Fatalf("Submit(invalid): got (%v, %v), want (nil, %v)", handle, err, ErrPriorityInvalid)
		}
	})

	t.Run("try_enqueue_or_error_invalid", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		handle, err := pq.Submit(context.Background(), func(ctx context.Context) error { return nil }, Priority(999), WithNonBlocking())
		if handle != nil {
			t.Error("Submit(invalid): got handle, want nil")
		}
		if !errors.Is(err, ErrPriorityInvalid) {
			t.Fatalf("Submit(invalid): got err=%v, want %v", err, ErrPriorityInvalid)
		}
	})

	t.Run("try_enqueue_or_error_full", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 1)
		defer pq.Stop(false)

		job := func(ctx context.Context) error { return nil }
		if handle, err := pq.Submit(context.Background(), job, PriorityHigh, WithNonBlocking()); handle == nil || err != nil {
			t.Fatalf("Submit(non-blocking): first submission got (%v,%v), want (handle,nil)", handle, err)
		}

		handle, err := pq.Submit(context.Background(), job, PriorityHigh, WithNonBlocking())
		if handle != nil {
			t.Error("Submit(non-blocking): got handle on full channel, want nil")
		}
		if !errors.Is(err, ErrPriorityQueueFull) {
			t.Fatalf("Submit(non-blocking): got err=%v, want %v", err, ErrPriorityQueueFull)
		}
	})

	t.Run("enqueue_or_error_stopped", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		pq.Stop(false)

		_, err := pq.Submit(context.Background(), func(ctx context.Context) error { return nil }, PriorityHigh)
		if !errors.Is(err, ErrPriorityQueueStopped) {
			t.Fatalf("Submit(): got err=%v, want %v", err, ErrPriorityQueueStopped)
		}
	})

	t.Run("try_enqueue_full", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 2) // Small capacity.
		defer pq.Stop(false)

		// Fill the high priority channel.
		blockingJob := func(ctx context.Context) error {
			time.Sleep(1 * time.Second)
			return nil
		}

		mustPrioritySubmit(t, pq, blockingJob, PriorityHigh)
		mustPrioritySubmit(t, pq, blockingJob, PriorityHigh)

		// Channel should now be full.
		if handle, err := pq.Submit(context.Background(), blockingJob, PriorityHigh, WithNonBlocking()); handle != nil || !errors.Is(err, ErrPriorityQueueFull) {
			t.Fatalf("Submit(non-blocking): got (%v, %v), want (nil, %v)", handle, err, ErrPriorityQueueFull)
		}
	})

	t.Run("delay_enqueue_priority", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		var executed atomic.Bool
		job := func(ctx context.Context) error {
			executed.Store(true)
			return nil
		}

		delay := 100 * time.Millisecond
		if _, err := pq.SubmitAfter(context.Background(), job, PriorityHigh, delay); err != nil {
			t.Fatalf("SubmitAfter(): unexpected err: %v", err)
		}

		// Should not be executed immediately.
		time.Sleep(50 * time.Millisecond)
		if executed.Load() {
			t.Error("SubmitAfter(): job should not have executed yet")
		}

		// Should be executed after delay.
		time.Sleep(100 * time.Millisecond)
		if !executed.Load() {
			t.Error("SubmitAfter(): job should have executed after delay")
		}
	})

	t.Run("count_by_priority", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		slowJob := func(ctx context.Context) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}

		// Submit jobs at different priorities.
		mustPrioritySubmit(t, pq, slowJob, PriorityHighest)
		mustPrioritySubmit(t, pq, slowJob, PriorityHighest)
		mustPrioritySubmit(t, pq, slowJob, PriorityHigh)
		mustPrioritySubmit(t, pq, slowJob, PriorityMedium)

		// Allow jobs to queue up before dispatcher pulls them.
		time.Sleep(10 * time.Millisecond)

		// Check counts (some may have been dispatched).
		highestCount := pq.CountByPriority(PriorityHighest)
		highCount := pq.CountByPriority(PriorityHigh)
		mediumCount := pq.CountByPriority(PriorityMedium)

		// At least verify the method returns non-negative values.
		if highestCount < 0 || highCount < 0 || mediumCount < 0 {
			t.Error("CountByPriority(): should return non-negative values")
		}
	})

	t.Run("pending_by_priority", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10)
		defer pq.Stop(false)

		slowJob := func(ctx context.Context) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}

		// Submit jobs at different priorities.
		mustPrioritySubmit(t, pq, slowJob, PriorityHighest)
		mustPrioritySubmit(t, pq, slowJob, PriorityHigh)
		mustPrioritySubmit(t, pq, slowJob, PriorityMedium)

		time.Sleep(10 * time.Millisecond)

		// Get pending map.
		pending := pq.PendingByPriority()

		// Verify map has all priority levels.
		if len(pending) != 5 {
			t.Errorf("PendingByPriority(): got %d entries, want 5", len(pending))
		}

		// Verify all priority levels are present.
		for _, priority := range []Priority{PriorityHighest, PriorityHigh, PriorityMedium, PriorityLow, PriorityLowest} {
			if _, exists := pending[priority]; !exists {
				t.Errorf("PendingByPriority(): missing priority %s", priority.String())
			}
		}
	})

	t.Run("drain", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		// Use long tick to prevent dispatcher from pulling jobs.
		pq := NewPriorityQueue(queue, 10, WithPriorityTick(10*time.Second))

		var executed atomic.Int32
		job := func(ctx context.Context) error {
			executed.Add(1)
			return nil
		}

		// Submit jobs at different priorities.
		mustPrioritySubmit(t, pq, job, PriorityHighest)
		mustPrioritySubmit(t, pq, job, PriorityHighest)
		mustPrioritySubmit(t, pq, job, PriorityHigh)
		mustPrioritySubmit(t, pq, job, PriorityMedium)
		mustPrioritySubmit(t, pq, job, PriorityLow)

		// Drain all buffered jobs.
		drained := pq.Drain()
		if drained != 5 {
			t.Errorf("Drain(): got %d drained, want 5", drained)
		}

		// Wait for drained jobs to execute.
		time.Sleep(100 * time.Millisecond)

		if executed.Load() != 5 {
			t.Errorf("Drain(): got %d executed, want 5", executed.Load())
		}

		// Stop without stopping queue (we handle it via defer).
		pq.Stop(false)
	})

	t.Run("drain_empty", func(t *testing.T) {
		queue := NewQueue(1, 5, 100)
		queue.Start()
		defer queue.Stop(true)

		pq := NewPriorityQueue(queue, 10, WithPriorityTick(10*time.Second))
		defer pq.Stop(false)

		// Drain empty queue.
		drained := pq.Drain()
		if drained != 0 {
			t.Errorf("Drain(): got %d drained from empty queue, want 0", drained)
		}
	})
}

func TestPriorityQueueSubmit_PreservesHandleThroughPriorityBuffer(t *testing.T) {
	queue := NewQueue(1, 1, 10)
	queue.Start()
	defer queue.Stop(true)

	pq := NewPriorityQueue(queue, 10, WithPriorityTick(time.Millisecond))
	defer pq.Stop(false)

	handle, err := pq.Submit(
		context.Background(),
		func(context.Context) error { return nil },
		PriorityHigh,
		WithJobID("priority-job"),
		WithJobAttribute("source", "priority"),
	)
	if err != nil {
		t.Fatalf("Submit(): %v", err)
	}
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait(): %v", err)
	}
	result, ok := handle.Result()
	if !ok {
		t.Fatal("Result(): submission should be complete")
	}
	if result.Meta.ID != "priority-job" || result.Meta.Attributes["source"] != "priority" {
		t.Fatalf("Result().Meta: got %+v", result.Meta)
	}
}

func TestPriorityQueueStop_RejectsBufferedSubmission(t *testing.T) {
	queue := NewQueue(0, 0, 0)
	queue.Start()
	defer queue.Stop(false)

	pq := NewPriorityQueue(queue, 1, WithPriorityTick(time.Hour))
	handle := mustPrioritySubmit(t, pq, func(context.Context) error { return nil }, PriorityHigh)

	pq.Stop(false)

	if err := handle.Wait(context.Background()); !errors.Is(err, ErrPriorityQueueStopped) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrPriorityQueueStopped)
	}
}

func TestPriorityQueueSubmit_ContextCancelsBufferAcceptance(t *testing.T) {
	queue := NewQueue(0, 0, 0)
	queue.Start()
	defer queue.Stop(false)

	pq := NewPriorityQueue(queue, 1, WithPriorityTick(time.Hour))
	defer pq.Stop(false)
	mustPrioritySubmit(t, pq, func(context.Context) error { return nil }, PriorityHigh)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	handle, err := pq.Submit(ctx, func(context.Context) error { return nil }, PriorityHigh)
	if handle != nil {
		t.Fatal("Submit(): got handle, want nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Submit(): got %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestPriorityQueueSubmit_CancelBufferedPreventsExecution(t *testing.T) {
	queue := NewQueue(1, 1, 1)
	queue.Start()
	defer queue.Stop(true)

	pq := NewPriorityQueue(queue, 1, WithPriorityTick(time.Hour))
	defer pq.Stop(false)

	ran := make(chan struct{}, 1)
	handle := mustPrioritySubmit(t, pq, func(context.Context) error {
		ran <- struct{}{}
		return nil
	}, PriorityHigh)

	if !handle.Cancel() {
		t.Fatal("Cancel(): got false, want true")
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrJobCancelled) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrJobCancelled)
	}
	if pq.trySubmit(pq.high) {
		t.Fatal("trySubmit(): forwarded cancelled submission")
	}
	select {
	case <-ran:
		t.Fatal("cancelled priority job executed")
	default:
	}
}

func TestPriorityQueueSubmitAfter_CancelPreventsSubmission(t *testing.T) {
	queue := NewQueue(1, 1, 1)
	queue.Start()
	defer queue.Stop(true)

	pq := NewPriorityQueue(queue, 1, WithPriorityTick(time.Hour))
	defer pq.Stop(false)

	ran := make(chan struct{}, 1)
	handle, err := pq.SubmitAfter(context.Background(), func(context.Context) error {
		ran <- struct{}{}
		return nil
	}, PriorityHigh, time.Hour)
	if err != nil {
		t.Fatalf("SubmitAfter(): got err=%v, want nil", err)
	}

	if !handle.Cancel() {
		t.Fatal("Cancel(): got false, want true")
	}
	if err := handle.Wait(context.Background()); !errors.Is(err, ErrJobCancelled) {
		t.Fatalf("Wait(): got %v, want %v", err, ErrJobCancelled)
	}
	select {
	case <-ran:
		t.Fatal("cancelled delayed priority job executed")
	default:
	}
}

func mustPrioritySubmit(t *testing.T, pq *PriorityQueue, job Job, priority Priority, opts ...SubmitOption) *JobHandle {
	t.Helper()
	handle, err := pq.Submit(context.Background(), job, priority, opts...)
	if err != nil {
		t.Fatalf("PriorityQueue.Submit(): %v", err)
	}
	return handle
}

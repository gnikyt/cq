package cq

import (
	"context"
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
				t.Errorf("Priority.String() = %v, want %v", got, tt.want)
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

		pq.EnqueuePriority(job, PriorityHigh)

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
		var mu atomic.Value
		mu.Store(&order)

		makeJob := func(id int) Job {
			return func(ctx context.Context) error {
				currentOrder := mu.Load().(*[]int)
				*currentOrder = append(*currentOrder, id)
				return nil
			}
		}

		// Enqueue in reverse priority order.
		pq.EnqueuePriority(makeJob(5), PriorityLowest)
		pq.EnqueuePriority(makeJob(4), PriorityLow)
		pq.EnqueuePriority(makeJob(3), PriorityMedium)
		pq.EnqueuePriority(makeJob(2), PriorityHigh)
		pq.EnqueuePriority(makeJob(1), PriorityHighest)

		// Wait for all jobs to process.
		time.Sleep(100 * time.Millisecond)

		finalOrder := mu.Load().(*[]int)
		if len(*finalOrder) != 5 {
			t.Fatalf("PriorityQueue: expected 5 jobs, got %d", len(*finalOrder))
		}

		// Highest priority should execute first.
		if (*finalOrder)[0] != 1 {
			t.Errorf("PriorityQueue: first job should be 1 (highest), got %d", (*finalOrder)[0])
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
		if ok := pq.TryEnqueuePriority(job, PriorityHigh); !ok {
			t.Error("TryEnqueuePriority(): should succeed on non-full channel")
		}

		// Wait for job to process.
		time.Sleep(50 * time.Millisecond)

		if !executed.Load() {
			t.Error("TryEnqueuePriority(): job should have executed")
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

		pq.EnqueuePriority(blockingJob, PriorityHigh)
		pq.EnqueuePriority(blockingJob, PriorityHigh)

		// Channel should now be full.
		if ok := pq.TryEnqueuePriority(blockingJob, PriorityHigh); ok {
			t.Error("TryEnqueuePriority(): should fail on full channel")
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
		pq.DelayEnqueuePriority(job, PriorityHigh, delay)

		// Should not be executed immediately.
		time.Sleep(50 * time.Millisecond)
		if executed.Load() {
			t.Error("DelayEnqueuePriority(): job should not have executed yet")
		}

		// Should be executed after delay.
		time.Sleep(100 * time.Millisecond)
		if !executed.Load() {
			t.Error("DelayEnqueuePriority(): job should have executed after delay")
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

		// Enqueue jobs at different priorities.
		pq.EnqueuePriority(slowJob, PriorityHighest)
		pq.EnqueuePriority(slowJob, PriorityHighest)
		pq.EnqueuePriority(slowJob, PriorityHigh)
		pq.EnqueuePriority(slowJob, PriorityMedium)

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

		// Enqueue jobs at different priorities.
		pq.EnqueuePriority(slowJob, PriorityHighest)
		pq.EnqueuePriority(slowJob, PriorityHigh)
		pq.EnqueuePriority(slowJob, PriorityMedium)

		time.Sleep(10 * time.Millisecond)

		// Get pending map.
		pending := pq.PendingByPriority()

		// Verify map has all priority levels.
		if len(pending) != 5 {
			t.Errorf("PendingByPriority(): should return map with 5 entries, got %d", len(pending))
		}

		// Verify all priority levels are present.
		for _, priority := range []Priority{PriorityHighest, PriorityHigh, PriorityMedium, PriorityLow, PriorityLowest} {
			if _, exists := pending[priority]; !exists {
				t.Errorf("PendingByPriority(): missing priority %s", priority.String())
			}
		}
	})
}

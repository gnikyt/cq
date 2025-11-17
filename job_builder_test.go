package cq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestJobBuilder(t *testing.T) {
	t.Run("jobs_build", func(t *testing.T) {
		queue := NewQueue(1, 10, 100)

		job := NewJob(func(ctx context.Context) error {
			return nil
		}, queue).Build()

		if job == nil {
			t.Error("Build(): failed to build job")
		}
	})

	t.Run("layers_apply_in_order", func(t *testing.T) {
		queue := NewQueue(1, 10, 100)
		queue.Start()

		// Track the order wrappers are applied.
		order := []string{}

		// Custom layers to track wrapping order.
		layer1 := func(job Job) Job {
			return func(ctx context.Context) error {
				order = append(order, "layer1_before")
				err := job(ctx)
				order = append(order, "layer1_after")
				return err
			}
		}
		layer2 := func(job Job) Job {
			return func(ctx context.Context) error {
				order = append(order, "layer2_before")
				err := job(ctx)
				order = append(order, "layer2_after")
				return err
			}
		}
		layer3 := func(job Job) Job {
			return func(ctx context.Context) error {
				order = append(order, "layer3_before")
				err := job(ctx)
				order = append(order, "layer3_after")
				return err
			}
		}

		NewJob(func(ctx context.Context) error {
			order = append(order, "core_job")
			return nil
		}, queue).
			Then(layer1).
			Then(layer2).
			Then(layer3).
			Dispatch()

		// Wait for job to complete before checking results.
		queue.Stop(true)

		// Layers are applied in reverse order during Build.
		// Then(layer1).Then(layer2).Then(layer3) results in: layer1(layer2(layer3(core))).
		expected := []string{
			"layer1_before",
			"layer2_before",
			"layer3_before",
			"core_job",
			"layer3_after",
			"layer2_after",
			"layer1_after",
		}
		if len(order) != len(expected) {
			t.Errorf("JobBuilder layers: got %d steps, want %d", len(order), len(expected))
		}
		for i, step := range expected {
			if i >= len(order) || order[i] != step {
				t.Errorf("JobBuilder layers: step %d got %q, want %q", i, order[i], step)
			}
		}
	})

	t.Run("jobs_dispatch", func(t *testing.T) {
		queue := NewQueue(1, 10, 100)
		queue.Start()

		var executed atomic.Value
		NewJob(func(ctx context.Context) error {
			executed.Store(true)
			return nil
		}, queue).Dispatch()

		queue.Stop(true)

		if executed.Load() == nil || !executed.Load().(bool) {
			t.Error("Dispatch(): job was not executed")
		}
	})

	t.Run("dispatch_after", func(t *testing.T) {
		queue := NewQueue(1, 10, 100)
		queue.Start()

		start := time.Now()
		var executed atomic.Value

		NewJob(func(ctx context.Context) error {
			executed.Store(true)
			return nil
		}, queue).DispatchAfter(100 * time.Millisecond)

		// Wait for the delayed job to be enqueued and executed.
		time.Sleep(150 * time.Millisecond)
		queue.Stop(true)
		elapsed := time.Since(start)

		if executed.Load() == nil || !executed.Load().(bool) {
			t.Error("DispatchAfter(): job was not executed")
		}

		if elapsed < 100*time.Millisecond {
			t.Errorf("DispatchAfter(): job executed too early (%v)", elapsed)
		}
	})

	t.Run("try_dispatch", func(t *testing.T) {
		queue := NewQueue(1, 10, 100)
		queue.Start()
		defer queue.Stop(true)

		// TryDispatch should succeed when queue has capacity,
		success := NewJob(func(ctx context.Context) error {
			return nil
		}, queue).TryDispatch()

		if !success {
			t.Error("TryDispatch(): should succeed")
		}

		time.Sleep(50 * time.Millisecond)
	})
}

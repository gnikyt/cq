package cq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerEvery(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)

	var count atomic.Int32
	job := func(ctx context.Context) error {
		count.Add(1)
		return nil
	}

	// Schedule job to run every 50ms.
	err := scheduler.Every("test-job", 50*time.Millisecond, job)
	if err != nil {
		t.Fatalf("Every(): unexpected err: %v", err)
	}

	// Wait for multiple executions.
	time.Sleep(175 * time.Millisecond)
	scheduler.Stop()

	executions := count.Load()
	if executions < 3 {
		t.Errorf("Every(): got %d executions, want >= 3", executions)
	}
}

func TestSchedulerAt(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)

	var executed atomic.Bool
	job := func(ctx context.Context) error {
		executed.Store(true)
		return nil
	}

	// Schedule job to run once in 100ms.
	runAt := time.Now().Add(100 * time.Millisecond)
	err := scheduler.At("one-time-job", runAt, job)
	if err != nil {
		t.Fatalf("At(): unexpected err: %v", err)
	}

	// Job should exist before execution.
	if !scheduler.Has("one-time-job") {
		t.Error("At(): job should exist before execution")
	}

	// Wait for execution.
	time.Sleep(150 * time.Millisecond)

	if !executed.Load() {
		t.Error("At(): job was not executed")
	}

	// Job should be auto-removed after execution.
	time.Sleep(50 * time.Millisecond)
	if scheduler.Has("one-time-job") {
		t.Error("At(): job should be removed after execution")
	}

	scheduler.Stop()
}

func TestSchedulerDuplicateID(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)
	defer scheduler.Stop()

	job := func(ctx context.Context) error {
		return nil
	}

	// Add first job.
	err := scheduler.Every("duplicate", 1*time.Second, job)
	if err != nil {
		t.Fatalf("Every(): unexpected err: %v", err)
	}

	// Try to add job with same ID.
	err = scheduler.Every("duplicate", 1*time.Second, job)
	if err == nil {
		t.Error("Every(): expected error for duplicate ID, got nil")
	}

	// Same for At().
	err = scheduler.At("duplicate2", time.Now().Add(1*time.Hour), job)
	if err != nil {
		t.Fatalf("At(): unexpected err: %v", err)
	}

	err = scheduler.At("duplicate2", time.Now().Add(1*time.Hour), job)
	if err == nil {
		t.Error("At(): expected error for duplicate ID, got nil")
	}
}

func TestSchedulerRemove(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)
	defer scheduler.Stop()

	var count atomic.Int32
	job := func(ctx context.Context) error {
		count.Add(1)
		return nil
	}

	// Schedule job.
	err := scheduler.Every("removable", 50*time.Millisecond, job)
	if err != nil {
		t.Fatalf("Every(): unexpected err: %v", err)
	}

	// Let it run a few times.
	time.Sleep(125 * time.Millisecond)
	firstCount := count.Load()

	// Remove the job.
	removed := scheduler.Remove("removable")
	if !removed {
		t.Error("Remove(): expected true, got false")
	}

	// Wait and verify no more executions.
	time.Sleep(150 * time.Millisecond)
	finalCount := count.Load()

	if finalCount != firstCount {
		t.Errorf("Remove(): got %d executions after removal, want %d (no change)", finalCount, firstCount)
	}

	// Try to remove non-existent job.
	removed = scheduler.Remove("non-existent")
	if removed {
		t.Error("Remove(): expected false for non-existent job, got true")
	}
}

func TestSchedulerStop(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)

	var count atomic.Int32
	job := func(ctx context.Context) error {
		count.Add(1)
		return nil
	}

	// Schedule multiple jobs.
	err := scheduler.Every("job1", 50*time.Millisecond, job)
	if err != nil {
		t.Fatalf("Every(): unexpected err: %v", err)
	}

	err = scheduler.Every("job2", 50*time.Millisecond, job)
	if err != nil {
		t.Fatalf("Every(): unexpected err: %v", err)
	}

	// Let them run.
	time.Sleep(75 * time.Millisecond)
	countBeforeStop := count.Load()

	// Stop scheduler.
	scheduler.Stop()

	// Wait and verify no more executions.
	time.Sleep(150 * time.Millisecond)
	countAfterStop := count.Load()

	if countAfterStop != countBeforeStop {
		t.Errorf("Stop(): got %d executions after stop, want %d (no change)", countAfterStop, countBeforeStop)
	}
}

func TestSchedulerHas(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)
	defer scheduler.Stop()

	job := func(ctx context.Context) error {
		return nil
	}

	// Check non-existent job.
	if scheduler.Has("non-existent") {
		t.Error("Has(): expected false for non-existent job, got true")
	}

	// Add job and check.
	err := scheduler.Every("exists", 1*time.Second, job)
	if err != nil {
		t.Fatalf("Every(): unexpected err: %v", err)
	}

	if !scheduler.Has("exists") {
		t.Error("Has(): expected true for existing job, got false")
	}

	// Remove and check.
	scheduler.Remove("exists")
	if scheduler.Has("exists") {
		t.Error("Has(): expected false after removal, got true")
	}
}

func TestSchedulerCountAndList(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)
	defer scheduler.Stop()

	job := func(ctx context.Context) error {
		return nil
	}

	// Initial count.
	if count := scheduler.Count(); count != 0 {
		t.Errorf("Count(): got %d, want 0", count)
	}

	// Add jobs.
	scheduler.Every("job1", 1*time.Second, job)
	scheduler.Every("job2", 1*time.Second, job)
	scheduler.At("job3", time.Now().Add(1*time.Hour), job)

	// Check count.
	if count := scheduler.Count(); count != 3 {
		t.Errorf("Count(): got %d, want 3", count)
	}

	// Check list.
	list := scheduler.List()
	if len(list) != 3 {
		t.Errorf("List(): got %d jobs, want 3", len(list))
	}

	// Verify all IDs are present.
	found := make(map[string]bool)
	for _, id := range list {
		found[id] = true
	}

	for _, expectedID := range []string{"job1", "job2", "job3"} {
		if !found[expectedID] {
			t.Errorf("List(): missing job ID %s", expectedID)
		}
	}

	// Remove one and check.
	scheduler.Remove("job2")
	if count := scheduler.Count(); count != 2 {
		t.Errorf("Count(): got %d after removal, want 2", count)
	}
}

func TestSchedulerAtPastTime(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)
	defer scheduler.Stop()

	job := func(ctx context.Context) error {
		return nil
	}

	// Try to schedule in the past.
	pastTime := time.Now().Add(-1 * time.Hour)
	err := scheduler.At("past-job", pastTime, job)
	if err == nil {
		t.Error("At(): expected error for past time, got nil")
	}
}

func TestSchedulerEveryInvalidInterval(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)
	defer scheduler.Stop()

	job := func(ctx context.Context) error {
		return nil
	}

	// Try zero interval.
	err := scheduler.Every("zero", 0, job)
	if err == nil {
		t.Error("Every(): expected error for zero interval, got nil")
	}

	// Try negative interval.
	err = scheduler.Every("negative", -1*time.Second, job)
	if err == nil {
		t.Error("Every(): expected error for negative interval, got nil")
	}
}

func TestSchedulerErrorChecking(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	scheduler := NewScheduler(context.Background(), queue)
	defer scheduler.Stop()

	job := func(ctx context.Context) error {
		return nil
	}

	// Test ErrJobExists.
	err := scheduler.Every("duplicate", 1*time.Second, job)
	if err != nil {
		t.Fatalf("Every(): unexpected err on first call: %v", err)
	}

	err = scheduler.Every("duplicate", 1*time.Second, job)
	if !errors.Is(err, ErrJobExists) {
		t.Errorf("Every(): expected ErrJobExists, got %v", err)
	}

	// Test ErrInvalidInterval.
	err = scheduler.Every("invalid", 0, job)
	if !errors.Is(err, ErrInvalidInterval) {
		t.Errorf("Every(): expected ErrInvalidInterval for zero interval, got %v", err)
	}

	err = scheduler.Every("invalid2", -1*time.Second, job)
	if !errors.Is(err, ErrInvalidInterval) {
		t.Errorf("Every(): expected ErrInvalidInterval for negative interval, got %v", err)
	}

	// Test ErrScheduleInPast.
	pastTime := time.Now().Add(-1 * time.Hour)
	err = scheduler.At("past", pastTime, job)
	if !errors.Is(err, ErrScheduleInPast) {
		t.Errorf("At(): expected ErrScheduleInPast, got %v", err)
	}

	// Test ErrJobExists for At().
	futureTime := time.Now().Add(1 * time.Hour)
	err = scheduler.At("duplicate-at", futureTime, job)
	if err != nil {
		t.Fatalf("At(): unexpected err on first call: %v", err)
	}

	err = scheduler.At("duplicate-at", futureTime, job)
	if !errors.Is(err, ErrJobExists) {
		t.Errorf("At(): expected ErrJobExists, got %v", err)
	}
}

func TestSchedulerContextCancellation(t *testing.T) {
	queue := NewQueue(1, 10, 100)
	queue.Start()
	defer queue.Stop(true)

	// Create cancellable context.
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := NewScheduler(ctx, queue)

	var count atomic.Int32
	job := func(ctx context.Context) error {
		count.Add(1)
		return nil
	}

	// Schedule recurring job.
	err := scheduler.Every("auto-cancel", 50*time.Millisecond, job)
	if err != nil {
		t.Fatalf("Every(): unexpected err: %v", err)
	}

	// Let it run a few times.
	time.Sleep(125 * time.Millisecond)
	firstCount := count.Load()

	// Cancel context (should stop scheduler).
	cancel()
	time.Sleep(50 * time.Millisecond) // Give time for cancellation to propagate.

	// Wait and verify no more executions.
	time.Sleep(150 * time.Millisecond)
	finalCount := count.Load()

	if finalCount != firstCount {
		t.Errorf("ContextCancellation(): got %d executions after cancel, want %d (no change)", finalCount, firstCount)
	}
}

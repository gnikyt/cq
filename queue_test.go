package cq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewQueueValidation(t *testing.T) {
	tests := []struct {
		name      string
		wmin      int
		wmax      int
		cap       int
		wantPanic string
	}{
		{
			name:      "negative wmin",
			wmin:      -1,
			wmax:      10,
			cap:       100,
			wantPanic: "cq: wmin must be >= 0",
		},
		{
			name:      "wmax less than wmin",
			wmin:      10,
			wmax:      5,
			cap:       100,
			wantPanic: "cq: wmax must be >= wmin",
		},
		{
			name:      "negative cap",
			wmin:      1,
			wmax:      10,
			cap:       -1,
			wantPanic: "cq: cap must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("NewQueue(%d, %d, %d) did not panic", tt.wmin, tt.wmax, tt.cap)
					return
				}
				if r != tt.wantPanic {
					t.Errorf("NewQueue(%d, %d, %d): got panic %v, want %v", tt.wmin, tt.wmax, tt.cap, r, tt.wantPanic)
				}
			}()
			NewQueue(tt.wmin, tt.wmax, tt.cap)
		})
	}
}

func TestNewQueueValidValues(t *testing.T) {
	// These should not panic.
	tests := []struct {
		name string
		wmin int
		wmax int
		cap  int
	}{
		{"zero workers", 0, 0, 0},
		{"equal min max", 5, 5, 10},
		{"normal values", 1, 10, 100},
		{"zero cap", 1, 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQueue(tt.wmin, tt.wmax, tt.cap)
			if q == nil {
				t.Errorf("NewQueue(%d, %d, %d) returned nil", tt.wmin, tt.wmax, tt.cap)
			}
		})
	}
}

func TestQueueGetters(t *testing.T) {
	tests := []struct {
		name string
		want func(*Queue) error
	}{
		{
			name: "capacity",
			want: func(q *Queue) error {
				want := 1
				if c := q.Capacity(); c != want {
					return fmt.Errorf("Capacity(): got %v, want %v", c, want)
				}
				return nil
			},
		},
		{
			name: "running",
			want: func(q *Queue) error {
				want := 1
				if rw := q.RunningWorkers(); rw != want {
					return fmt.Errorf("RunningWorkers(): got %v, want %v", rw, want)
				}
				return nil
			},
		},
		{
			name: "idle",
			want: func(q *Queue) error {
				want := 1
				if iw := q.IdleWorkers(); iw != want {
					return fmt.Errorf("IdleWorkers(): got %v, want %v", iw, want)
				}
				return nil
			},
		},
		{
			name: "range",
			want: func(q *Queue) error {
				wantMin := 1
				wantMax := 2
				wmin, wmax := q.WorkerRange()
				if wmin != wantMin || wmax != wantMax {
					return fmt.Errorf("WorkerRange(): got %v:%v, want %v:%v", wmin, wmax, wantMin, wantMax)
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		q := NewQueue(1, 2, 1)
		q.Start()
		defer q.Stop(false)

		t.Run(tt.name, func(t *testing.T) {
			if err := tt.want(q); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestQueueEnqueue(t *testing.T) {
	var called atomic.Bool // Was job called?
	q := NewQueue(1, 1, 1)
	q.Start()

	q.Enqueue(func(ctx context.Context) error {
		called.Store(true)
		return nil
	})
	q.Stop(true)

	if !called.Load() {
		t.Error("Enqueue(): expected job to enqueued and executed")
	}
}

func TestQueueDelayEnqueue(t *testing.T) {
	delay := time.Duration(500 * time.Millisecond)
	sleep := time.Duration(600 * time.Millisecond)

	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)
	q.DelayEnqueue(
		func(ctx context.Context) error {
			return nil
		},
		delay,
	)

	if njc := q.TallyOf(JobStateCreated); njc != 0 {
		t.Errorf("DelayQueue(): TallyOf(JobStateCreated): got %v, want 0 (job not created yet)", njc)
	}
	time.Sleep(sleep) // Give job time to enqueue and run.
	if njc := q.TallyOf(JobStateCreated); njc != 1 {
		t.Errorf("DelayQueue(): TallyOf(JobStateCreated): got %v, want 1", njc)
	}
}

func TestQueueEnqueueBatch(t *testing.T) {
	q := NewQueue(2, 5, 10)
	q.Start()
	defer q.Stop(true)

	var called1 atomic.Bool
	var called2 atomic.Bool
	var called3 atomic.Bool

	jobs := []Job{
		func(ctx context.Context) error {
			called1.Store(true)
			return nil
		},
		func(ctx context.Context) error {
			called2.Store(true)
			return nil
		},
		func(ctx context.Context) error {
			called3.Store(true)
			return nil
		},
	}

	q.EnqueueBatch(jobs)

	// Wait for all jobs to complete.
	time.Sleep(100 * time.Millisecond)

	if !called1.Load() || !called2.Load() || !called3.Load() {
		t.Errorf(
			"EnqueueBatch(): jobs not all executed: called1=%v, called2=%v, called3=%v",
			called1.Load(), called2.Load(), called3.Load(),
		)
	}

	// Check tallies.
	if created := q.TallyOf(JobStateCreated); created != 3 {
		t.Errorf("EnqueueBatch(): TallyOf(JobStateCreated): got %d, want 3", created)
	}
	if completed := q.TallyOf(JobStateCompleted); completed != 3 {
		t.Errorf("EnqueueBatch(): TallyOf(JobStateCompleted): got %d, want 3", completed)
	}
}

func TestQueueDelayEnqueueBatch(t *testing.T) {
	delay := time.Duration(500 * time.Millisecond)
	sleep := time.Duration(600 * time.Millisecond)

	q := NewQueue(2, 5, 10)
	q.Start()
	defer q.Stop(true)

	jobs := []Job{
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
	}

	q.DelayEnqueueBatch(jobs, delay)

	// Jobs should not be created yet.
	if njc := q.TallyOf(JobStateCreated); njc != 0 {
		t.Errorf("DelayEnqueueBatch(): TallyOf(JobStateCreated): got %v, want 0 (jobs not created yet)", njc)
	}

	// Wait for jobs to enqueue and run.
	time.Sleep(sleep)

	// All jobs should now be created.
	if njc := q.TallyOf(JobStateCreated); njc != 3 {
		t.Errorf("DelayEnqueueBatch(): TallyOf(JobStateCreated): got %v, want 3", njc)
	}
	if completed := q.TallyOf(JobStateCompleted); completed != 3 {
		t.Errorf("DelayEnqueueBatch(): TallyOf(JobStateCompleted): got %d, want 3", completed)
	}
}

func TestQueueEnqueueIfStopped(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	q.Stop(true)

	job := func(ctx context.Context) error { return nil }
	if ok := q.TryEnqueue(job); ok {
		t.Error("TryEnqueue(): got true, want false")
	}
}

func TestQueueTallies(t *testing.T) {
	tests := []struct {
		state JobState
		job   Job
		want  func(*Queue) error
	}{
		{
			state: JobStateCompleted,
			job: func(ctx context.Context) error {
				return nil
			},
			want: func(q *Queue) error {
				want := 1
				if jc := q.TallyOf(JobStateCompleted); jc != want {
					return fmt.Errorf("TallyOf(JobStateCompleted): got %v, want %v", jc, want)
				}
				if jc := q.TallyOf(JobStateCreated); jc != want {
					return fmt.Errorf("TallyOf(JobStateCreated): got %v, want %v", jc, want)
				}
				return nil
			},
		},
		{
			state: JobStateFailed,
			job: func(ctx context.Context) error {
				return errors.New("error")
			},
			want: func(q *Queue) error {
				want := 1
				if jc := q.TallyOf(JobStateFailed); jc != want {
					return fmt.Errorf("TallyOf(JobStateFailed): got %v, want %v", jc, want)
				}
				if jc := q.TallyOf(JobStateCreated); jc != want {
					return fmt.Errorf("TallyOf(JobStateCreated): got %v, want %v", jc, want)
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			q := NewQueue(1, 1, 1)
			q.Start()
			q.Enqueue(tt.job)
			q.Stop(true)
			if err := tt.want(q); err != nil {
				t.Errorf("%T: %v", tt.state, err)
			}
		})
	}
}

func TestQueuePanic(t *testing.T) {
	var called atomic.Bool

	q := NewQueue(1, 1, 1, WithPanicHandler(func(err any) {
		called.Store(true)
	}))
	q.Start()
	q.Enqueue(func(ctx context.Context) error {
		panic("panic")
	})
	q.Stop(true)

	if !called.Load() {
		t.Error("WithPanicHandler(): should have executed")
	}
}

func TestIdleWorkerTick(t *testing.T) {
	var atMax bool                                                  // Running workers at max?
	wmin, wmax := 1, 2                                              // Min. & max. workers.
	jobs := 50                                                      // Number of jobs.
	delay := time.Duration(50 * time.Millisecond)                   // Delay of each job.
	idle := time.Duration(150 * time.Millisecond)                   // Idle tick.
	sleep := (time.Duration(jobs * int(delay))) + (2 * time.Second) // Check delay with 2s buffer.

	q := NewQueue(wmin, wmax, 0, WithWorkerIdleTick(idle))
	q.Start()
	defer q.Stop(true)

	for i := 0; i < jobs; i++ {
		q.Enqueue(func(ctx context.Context) error {
			time.Sleep(delay)
			return nil
		})

		// At some point, we should have wmax workers.
		if !atMax {
			if rw := q.RunningWorkers(); rw == wmax {
				atMax = true
			}
		}
	}

	if !atMax {
		// Because so many jobs were spammed, this should be more than worker min.
		t.Error("Queue: did not spin up the desired workers")
	}
	time.Sleep(sleep) // Let the cleanup happen.
	// Should be back to worker min.
	if nrw := q.RunningWorkers(); nrw != wmin {
		t.Errorf("RunningWorkers(): got %v, want %v", nrw, wmin)
	}
}

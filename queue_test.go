package cq

// TODO: A lot of these tests rely on time-based checks.
// Move to a different method later.

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestQueueGetters(t *testing.T) {
	tests := []struct {
		name string
		want func(*Queue) bool
	}{
		{
			name: "capacity",
			want: func(q *Queue) bool {
				return q.Capacity() == 1
			},
		},
		{
			name: "running",
			want: func(q *Queue) bool {
				return q.RunningWorkers() == 1
			},
		},
		{
			name: "idle",
			want: func(q *Queue) bool {
				return q.IdleWorkers() == 1
			},
		},
		{
			name: "range",
			want: func(q *Queue) bool {
				wmin, wmax := q.WorkerRange()
				return wmin == 1 && wmax == 2
			},
		},
	}
	for _, tt := range tests {
		q := NewQueue(1, 2, 1)
		q.Start()
		defer q.Stop(false)
		t.Run(tt.name, func(t *testing.T) {
			if ok := tt.want(q); !ok {
				t.Error("should be true")
			}
		})
	}
}

func TestQueueEnqueue(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()

	var called bool
	q.Enqueue(func() error {
		called = true
		return nil
	})
	q.Stop(true)

	if !called {
		t.Error("Enqueue(): expected job to enqueued and ran")
	}
}

func TestQueueDelayEnqueue(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	defer q.Stop(true)

	q.DelayEnqueue(func() error {
		return nil
	}, time.Duration(500)*time.Millisecond)
	if n := q.TallyOf(JobStateCreated); n != 0 {
		t.Errorf("DelayQueue(): should not have created job, got %v, want 0", n)
	}

	time.Sleep(time.Duration(550) * time.Millisecond)
	if n := q.TallyOf(JobStateCreated); n != 1 {
		t.Errorf("DelayQueue(): should have created job, got %v, want 1", n)
	}
}

func TestQueueEnqueueMaxed(t *testing.T) {
	var panicm string
	q := NewQueue(1, 1, 0, WithPanicHandler(func(err interface{}) {
		panicm = fmt.Sprintf("%v", err)
	}))

	q.Start()
	for i := 0; i < 500; i++ { // Spam the queue with 0 capacity.
		go func() {
			q.Enqueue(func() error {
				time.Sleep(time.Duration(500) * time.Millisecond)
				return nil
			})
		}()
	}
	q.Stop(false)

	<-time.After(time.Duration(1) * time.Second) // Allow panic to run
	if panicm != "send on closed channel" {
		t.Error("expected panic to happen")
	}
}

func TestQueueEnqueueIfStopped(t *testing.T) {
	q := NewQueue(1, 1, 1)
	q.Start()
	q.Stop(true)

	if ok := q.TryEnqueue(func() error { return nil }); ok {
		t.Error("TryEnqueue() = true, want false")
	}
}

func TestQueueTallies(t *testing.T) {
	tests := []struct {
		state JobState
		job   Job
		want  func(*Queue) bool
	}{
		{
			state: JobStateCompleted,
			job: func() error {
				return nil
			},
			want: func(q *Queue) bool {
				return q.TallyOf(JobStateCompleted) == 1 && q.TallyOf(JobStateCreated) == 1
			},
		},
		{
			state: JobStateFailed,
			job: func() error {
				return errors.New("error")
			},
			want: func(q *Queue) bool {
				return q.TallyOf(JobStateFailed) == 1 && q.TallyOf(JobStateCreated) == 1
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			q := NewQueue(1, 1, 1)
			q.Start()
			q.Enqueue(tt.job)
			q.Stop(true)
			if !tt.want(q) {
				t.Errorf("TallyOf('%v') = false, want true", tt.state)
			}
		})
	}
}

func TestQueuePanic(t *testing.T) {
	var called bool
	q := NewQueue(1, 1, 1, WithPanicHandler(func(err interface{}) {
		called = true
	}))
	q.Start()
	q.Enqueue(func() error { panic("panic") })
	q.Stop(true)

	if !called {
		t.Error("panic handler should have ran, it did not")
	}
}

func TestIdleWorkerTick(t *testing.T) {
	var rw int
	wmin := 1
	wmax := 2
	q := NewQueue(wmin, wmax, 100, WithWorkerIdleTick(time.Duration(50)*time.Millisecond))
	q.Start()
	defer q.Stop(false)

	q.Enqueue(func() error {
		time.Sleep(time.Duration(500) * time.Millisecond)
		return nil
	})
	for i := 0; i < 100; i++ {
		go func() {
			q.Enqueue(func() error {
				return nil
			})
		}()
		rw = q.RunningWorkers()
	}

	if rw > wmin {
		// Because so many jobs were spammed, this should be more than worker min.
		t.Errorf("RunningWorkers() = %v, want atleast %v", rw, 2)
	}
	// Let the cleanup happen.
	<-time.After(time.Duration(2) * time.Second)
	// Should be back to worker min.
	if nrw := q.RunningWorkers(); nrw != wmin {
		t.Errorf("RunningWorkers() = %v, want %v", nrw, wmin)
	}
}

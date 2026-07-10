package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testPauseStore struct {
	mu    sync.RWMutex
	state map[string]bool
}

func newTestPauseStore() *testPauseStore {
	return &testPauseStore{
		state: make(map[string]bool),
	}
}

func (s *testPauseStore) IsPaused(_ context.Context, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state[key], nil
}

func (s *testPauseStore) SetPaused(_ context.Context, key string, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[key] = paused
	return nil
}

func TestQueuePauseResume_Local(t *testing.T) {
	var called atomic.Bool

	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	if err := q.Pause(); err != nil {
		t.Fatalf("Pause(): unexpected error: %v", err)
	}
	if !q.IsPaused() {
		t.Fatal("IsPaused(): got false, want true")
	}

	mustSubmit(t, q, func(ctx context.Context) error {
		called.Store(true)
		return nil
	})

	time.Sleep(80 * time.Millisecond)
	if called.Load() {
		t.Fatal("job executed while queue paused")
	}

	if err := q.Resume(); err != nil {
		t.Fatalf("Resume(): unexpected error: %v", err)
	}
	if q.IsPaused() {
		t.Fatal("IsPaused(): got true, want false")
	}

	waitDeadline := time.Now().Add(500 * time.Millisecond)
	for !called.Load() && time.Now().Before(waitDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !called.Load() {
		t.Fatal("job did not execute after resume")
	}
}

func TestQueuePause_ActiveJobContinues(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	done := make(chan struct{}, 1)

	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	mustSubmit(t, q, func(ctx context.Context) error {
		started <- struct{}{}
		<-release
		done <- struct{}{}
		return nil
	})

	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("job never started")
	}

	if err := q.Pause(); err != nil {
		t.Fatalf("Pause(): unexpected error: %v", err)
	}

	close(release)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("active job did not complete while paused")
	}
}

func TestQueuePauseResume_Distributed(t *testing.T) {
	store := newTestPauseStore()
	key := "orders"
	poll := 10 * time.Millisecond

	q1 := NewQueue(1, 1, 10, WithPauseStore(store, key), WithPausePollTick(poll))
	q2 := NewQueue(1, 1, 10, WithPauseStore(store, key), WithPausePollTick(poll))
	q1.Start()
	q2.Start()
	defer q1.Stop(true)
	defer q2.Stop(true)

	if err := q1.Pause(); err != nil {
		t.Fatalf("q1 Pause(): unexpected error: %v", err)
	}

	waitPausedDeadline := time.Now().Add(300 * time.Millisecond)
	for !q2.IsPaused() && time.Now().Before(waitPausedDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !q2.IsPaused() {
		t.Fatal("q2 did not observe distributed paused state")
	}

	var called atomic.Bool
	mustSubmit(t, q2, func(ctx context.Context) error {
		called.Store(true)
		return nil
	})

	time.Sleep(80 * time.Millisecond)
	if called.Load() {
		t.Fatal("job executed while distributed pause was active")
	}

	if err := q1.Resume(); err != nil {
		t.Fatalf("q1 Resume(): unexpected error: %v", err)
	}

	waitResumedDeadline := time.Now().Add(300 * time.Millisecond)
	for q2.IsPaused() && time.Now().Before(waitResumedDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if q2.IsPaused() {
		t.Fatal("q2 did not observe distributed resumed state")
	}

	waitCalledDeadline := time.Now().Add(500 * time.Millisecond)
	for !called.Load() && time.Now().Before(waitCalledDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !called.Load() {
		t.Fatal("job did not execute after distributed resume")
	}
}

func TestQueuePause_Stopped(t *testing.T) {
	q := NewQueue(1, 1, 10)
	q.Start()
	q.Stop(true)

	if err := q.Pause(); !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("Pause(): got %v, want %v", err, ErrQueueStopped)
	}
}

func TestQueueResume_Stopped(t *testing.T) {
	q := NewQueue(1, 1, 10)
	q.Start()
	q.Stop(true)

	if err := q.Resume(); !errors.Is(err, ErrQueueStopped) {
		t.Fatalf("Resume(): got %v, want %v", err, ErrQueueStopped)
	}
}

func TestQueuePauseReject_NonBlockingSubmit(t *testing.T) {
	q := NewQueue(1, 1, 10, WithPauseBehavior(PauseReject))
	q.Start()
	defer q.Stop(true)

	if err := q.Pause(); err != nil {
		t.Fatalf("Pause(): unexpected error: %v", err)
	}

	handle, err := q.Submit(context.Background(), func(ctx context.Context) error { return nil }, WithNonBlocking())
	if handle != nil || !errors.Is(err, ErrQueuePaused) {
		t.Fatalf("Submit(): got (%v, %v), want (nil, %v)", handle, err, ErrQueuePaused)
	}
}

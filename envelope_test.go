package cq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type testEnvelopeStore struct {
	mut      sync.Mutex
	enqueued []Envelope
	claimed  []string
	acked    []string
	nacked   []string
	resched  []Envelope
}

func (s *testEnvelopeStore) Enqueue(ctx context.Context, env Envelope) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.enqueued = append(s.enqueued, env)
	return nil
}

func (s *testEnvelopeStore) Claim(ctx context.Context, id string) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.claimed = append(s.claimed, id)
	return nil
}

func (s *testEnvelopeStore) Ack(ctx context.Context, id string) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.acked = append(s.acked, id)
	return nil
}

func (s *testEnvelopeStore) Nack(ctx context.Context, id string, reason error) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.nacked = append(s.nacked, id)
	return nil
}

func (s *testEnvelopeStore) Recoverable(ctx context.Context, now time.Time) ([]Envelope, error) {
	return nil, nil
}

func (s *testEnvelopeStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.resched = append(s.resched, Envelope{
		ID:        id,
		NextRunAt: nextRunAt,
		LastError: reason,
	})
	return nil
}

func TestQueueEnvelopeStore_SuccessPath(t *testing.T) {
	store := &testEnvelopeStore{}
	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()
	q.Enqueue(func(ctx context.Context) error { return nil })
	q.Stop(true)

	if len(store.enqueued) != 1 {
		t.Fatalf("got enqueued=%d, want 1", len(store.enqueued))
	}
	if len(store.acked) != 1 {
		t.Fatalf("got acked=%d, want 1", len(store.acked))
	}
	if len(store.claimed) != 1 {
		t.Fatalf("got claimed=%d, want 1", len(store.claimed))
	}
	if len(store.nacked) != 0 {
		t.Fatalf("got nacked=%d, want 0", len(store.nacked))
	}
	if store.acked[0] != store.enqueued[0].ID {
		t.Fatalf("ack id %q != enqueued id %q", store.acked[0], store.enqueued[0].ID)
	}
}

func TestQueueEnvelopeStore_FailurePath(t *testing.T) {
	store := &testEnvelopeStore{}
	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()
	q.Enqueue(func(ctx context.Context) error { return errors.New("boom") })
	q.Stop(true)

	if len(store.enqueued) != 1 {
		t.Fatalf("got enqueued=%d, want 1", len(store.enqueued))
	}
	if len(store.acked) != 0 {
		t.Fatalf("got acked=%d, want 0", len(store.acked))
	}
	if len(store.claimed) != 1 {
		t.Fatalf("got claimed=%d, want 1", len(store.claimed))
	}
	if len(store.nacked) != 1 {
		t.Fatalf("got nacked=%d, want 1", len(store.nacked))
	}
	if store.nacked[0] != store.enqueued[0].ID {
		t.Fatalf("nack id %q != enqueued id %q", store.nacked[0], store.enqueued[0].ID)
	}
}

func TestQueueEnvelopeStore_ClaimMatchesEnqueuedID(t *testing.T) {
	store := &testEnvelopeStore{}
	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()
	q.Enqueue(func(ctx context.Context) error { return nil })
	q.Stop(true)

	if len(store.enqueued) != 1 || len(store.claimed) != 1 {
		t.Fatalf("unexpected envelope counts: enq=%d claim=%d", len(store.enqueued), len(store.claimed))
	}
	if store.claimed[0] != store.enqueued[0].ID {
		t.Fatalf("claim id %q != enqueued id %q", store.claimed[0], store.enqueued[0].ID)
	}
}

func TestQueueEnvelopeStore_ReschedulePath(t *testing.T) {
	store := &testEnvelopeStore{}
	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()

	job := WithReleaseSelf(func(ctx context.Context) error {
		RequestRelease(ctx, 30*time.Millisecond)
		return nil
	}, q, 1)
	q.Enqueue(job)

	deadline := time.Now().Add(300 * time.Millisecond)
	for {
		store.mut.Lock()
		count := len(store.resched)
		store.mut.Unlock()
		if count > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	q.Stop(true)

	if len(store.resched) != 1 {
		t.Fatalf("got rescheduled=%d, want 1", len(store.resched))
	}
	if store.resched[0].ID == "" {
		t.Fatal("got empty reschedule id, want non-empty")
	}
	if store.resched[0].NextRunAt.IsZero() {
		t.Fatal("got zero next run time, want non-zero")
	}
	if store.resched[0].LastError != EnvelopeRescheduleReasonReleaseSelf {
		t.Fatalf("got reschedule reason=%q, want %q", store.resched[0].LastError, EnvelopeRescheduleReasonReleaseSelf)
	}
}

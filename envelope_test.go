package cq

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

func (s *testEnvelopeStore) RecoverByID(ctx context.Context, id string) (Envelope, error) {
	return Envelope{}, ErrEnvelopeNotFound
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

func (s *testEnvelopeStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	return nil
}

type payloadReplayStore struct {
	mut   sync.Mutex
	envs  map[string]Envelope
	order []string
}

func newPayloadReplayStore() *payloadReplayStore {
	return &payloadReplayStore{
		envs: make(map[string]Envelope),
	}
}

func (s *payloadReplayStore) Enqueue(ctx context.Context, env Envelope) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.envs[env.ID] = env
	s.order = append(s.order, env.ID)
	return nil
}

func (s *payloadReplayStore) Claim(ctx context.Context, id string) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	env := s.envs[id]
	env.Status = EnvelopeStatusClaimed
	env.ClaimedAt = time.Now()
	s.envs[id] = env
	return nil
}

func (s *payloadReplayStore) Ack(ctx context.Context, id string) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	env := s.envs[id]
	env.Status = EnvelopeStatusAcked
	s.envs[id] = env
	return nil
}

func (s *payloadReplayStore) Nack(ctx context.Context, id string, reason error) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	env := s.envs[id]
	env.Status = EnvelopeStatusNacked
	env.LastError = reason.Error()
	s.envs[id] = env
	return nil
}

func (s *payloadReplayStore) Recoverable(ctx context.Context, now time.Time) ([]Envelope, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	out := make([]Envelope, 0, len(s.envs))
	for _, id := range s.order {
		env := s.envs[id]
		if env.Status == EnvelopeStatusNacked {
			out = append(out, env)
		}
	}
	return out, nil
}

func (s *payloadReplayStore) RecoverByID(ctx context.Context, id string) (Envelope, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	env, ok := s.envs[id]
	if !ok {
		return Envelope{}, ErrEnvelopeNotFound
	}
	return env, nil
}

func (s *payloadReplayStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return nil
}

func (s *payloadReplayStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	env := s.envs[id]
	env.Type = typ
	env.Payload = append([]byte(nil), payload...)
	s.envs[id] = env
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

func TestQueueEnvelopeStore_SetEnvelopePayloadPersistsForReplay(t *testing.T) {
	store := newPayloadReplayStore()
	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()

	var didSet atomic.Bool
	q.Enqueue(func(ctx context.Context) error {
		didSet.Store(SetEnvelopePayload(ctx, "email", []byte("alpha")))
		return errors.New("boom")
	})
	q.Stop(true)

	if !didSet.Load() {
		t.Fatal("expected SetEnvelopePayload to return true")
	}

	envs, err := store.Recoverable(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("recoverable error: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("got recoverable=%d, want 1", len(envs))
	}
	if envs[0].Type != "email" {
		t.Fatalf("got type=%q, want %q", envs[0].Type, "email")
	}
	if !bytes.Equal(envs[0].Payload, []byte("alpha")) {
		t.Fatalf("got payload=%q, want %q", string(envs[0].Payload), "alpha")
	}
}

func TestQueueEnvelopeStore_SetEnvelopePayloadLastWriteWins(t *testing.T) {
	store := newPayloadReplayStore()
	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()

	q.Enqueue(func(ctx context.Context) error {
		_ = SetEnvelopePayload(ctx, "email", []byte("alpha"))
		_ = SetEnvelopePayload(ctx, "email_v2", []byte("beta"))
		return errors.New("boom")
	})
	q.Stop(true)

	envs, err := store.Recoverable(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("recoverable error: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("got recoverable=%d, want 1", len(envs))
	}
	if envs[0].Type != "email_v2" {
		t.Fatalf("got type=%q, want %q", envs[0].Type, "email_v2")
	}
	if !bytes.Equal(envs[0].Payload, []byte("beta")) {
		t.Fatalf("got payload=%q, want %q", string(envs[0].Payload), "beta")
	}
}

package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recoverStore struct {
	mut      sync.Mutex
	envs     []Envelope
	enqueued []Envelope
	err      error
}

func (s *recoverStore) Enqueue(ctx context.Context, env Envelope) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.enqueued = append(s.enqueued, env)
	return nil
}
func (s *recoverStore) Claim(ctx context.Context, id string) error { return nil }
func (s *recoverStore) Ack(ctx context.Context, id string) error   { return nil }
func (s *recoverStore) Nack(ctx context.Context, id string, reason error) error {
	return nil
}
func (s *recoverStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return nil
}
func (s *recoverStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	return nil
}
func (s *recoverStore) Recoverable(ctx context.Context, now time.Time) ([]Envelope, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.mut.Lock()
	defer s.mut.Unlock()
	out := make([]Envelope, len(s.envs))
	copy(out, s.envs)
	return out, nil
}
func (s *recoverStore) RecoverByID(ctx context.Context, id string) (Envelope, error) {
	if s.err != nil {
		return Envelope{}, s.err
	}
	s.mut.Lock()
	defer s.mut.Unlock()
	for _, env := range s.envs {
		if env.ID == id {
			return env, nil
		}
	}
	return Envelope{}, ErrEnvelopeNotFound
}

func TestRecoverEnvelopeByID_PreservesEnvelopeID(t *testing.T) {
	now := time.Now()
	store := &recoverStore{
		envs: []Envelope{
			{ID: "recover-id-1", Type: "email", Payload: []byte("alpha")},
		},
	}
	registry := NewEnvelopeRegistry()
	registry.Register("email", func(env Envelope) (Job, error) {
		return func(ctx context.Context) error { return nil }, nil
	})

	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()
	defer q.Stop(true)

	scheduled, err := RecoverEnvelopeByID(context.Background(), q, store, registry, "recover-id-1", now)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !scheduled {
		t.Fatal("expected scheduled=true")
	}

	deadline := time.Now().Add(1 * time.Second)
	for {
		store.mut.Lock()
		if len(store.enqueued) > 0 {
			gotID := store.enqueued[0].ID
			store.mut.Unlock()
			if gotID != "recover-id-1" {
				t.Fatalf("got enqueued id=%q, want %q", gotID, "recover-id-1")
			}
			return
		}
		store.mut.Unlock()

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for recovered enqueue")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRecoverEnvelopes_ReenqueuesRecoverableJobs(t *testing.T) {
	now := time.Now()
	store := &recoverStore{
		envs: []Envelope{
			{ID: "1", Type: "email", Payload: []byte("alpha")},
			{ID: "2", Type: "email", Payload: []byte("beta"), NextRunAt: now.Add(30 * time.Millisecond)},
		},
	}
	registry := NewEnvelopeRegistry()

	var executed atomic.Int32
	registry.Register("email", func(env Envelope) (Job, error) {
		return func(ctx context.Context) error {
			executed.Add(1)
			return nil
		}, nil
	})

	q := NewQueue(1, 1, 10)
	q.Start()
	count, err := RecoverEnvelopes(context.Background(), q, store, registry, now)
	if err != nil {
		t.Fatalf("recover error: %v", err)
	}
	if count != 2 {
		t.Fatalf("got scheduled=%d, want 2", count)
	}

	time.Sleep(80 * time.Millisecond)
	q.Stop(true)

	if got := executed.Load(); got != 2 {
		t.Fatalf("got executed=%d, want 2", got)
	}
}

func TestEnvelopeRegistry_FactoryFor(t *testing.T) {
	registry := NewEnvelopeRegistry()
	factory := func(env Envelope) (Job, error) {
		return func(ctx context.Context) error { return nil }, nil
	}
	registry.Register("email", factory)

	gotFactory, ok := registry.FactoryFor("email")
	if !ok {
		t.Fatal("expected factory to be found")
	}
	if gotFactory == nil {
		t.Fatal("expected non-nil factory")
	}

	gotMissing, ok := registry.FactoryFor("missing")
	if ok {
		t.Fatal("expected missing factory lookup to return ok=false")
	}
	if gotMissing != nil {
		t.Fatal("expected missing factory to be nil")
	}
}

func TestRecoverEnvelopes_MissingFactory(t *testing.T) {
	now := time.Now()
	store := &recoverStore{
		envs: []Envelope{{ID: "1", Type: "missing"}},
	}
	registry := NewEnvelopeRegistry()
	q := NewQueue(1, 1, 10)

	_, err := RecoverEnvelopes(context.Background(), q, store, registry, now)
	if err == nil {
		t.Fatal("expected error for missing factory")
	}
}

func TestRecoverEnvelopes_StoreError(t *testing.T) {
	expected := errors.New("db down")
	store := &recoverStore{err: expected}
	registry := NewEnvelopeRegistry()
	q := NewQueue(1, 1, 10)

	_, err := RecoverEnvelopes(context.Background(), q, store, registry, time.Now())
	if !errors.Is(err, expected) {
		t.Fatalf("got err=%v, want %v", err, expected)
	}
}

func TestRecoverEnvelopesWithOptions_ContinueOnError(t *testing.T) {
	now := time.Now()
	store := &recoverStore{
		envs: []Envelope{
			{ID: "1", Type: "missing"},
			{ID: "2", Type: "ok"},
			{ID: "3", Type: "bad"},
		},
	}
	registry := NewEnvelopeRegistry()
	registry.Register("ok", func(env Envelope) (Job, error) {
		return func(ctx context.Context) error { return nil }, nil
	})
	registry.Register("bad", func(env Envelope) (Job, error) {
		return nil, errors.New("decode failed")
	})

	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	var onErrCalls atomic.Int32
	report, err := RecoverEnvelopesWithOptions(context.Background(), q, store, registry, now, RecoverOptions{
		ContinueOnError: true,
		OnError: func(env Envelope, err error) {
			onErrCalls.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if report.Loaded != 3 {
		t.Fatalf("got loaded=%d, want 3", report.Loaded)
	}
	if report.SkippedUnknownType != 1 {
		t.Fatalf("got skipped=%d, want 1", report.SkippedUnknownType)
	}
	if report.FailedBuild != 1 {
		t.Fatalf("got failed_build=%d, want 1", report.FailedBuild)
	}
	if report.ScheduledNow != 1 {
		t.Fatalf("got scheduled_now=%d, want 1", report.ScheduledNow)
	}
	if got := onErrCalls.Load(); got != 2 {
		t.Fatalf("got on_error_calls=%d, want 2", got)
	}
}

func TestRecoverEnvelopesWithOptions_MaxEnvelopes(t *testing.T) {
	now := time.Now()
	store := &recoverStore{
		envs: []Envelope{
			{ID: "1", Type: "ok"},
			{ID: "2", Type: "ok"},
			{ID: "3", Type: "ok"},
		},
	}
	registry := NewEnvelopeRegistry()
	registry.Register("ok", func(env Envelope) (Job, error) {
		return func(ctx context.Context) error { return nil }, nil
	})

	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	report, err := RecoverEnvelopesWithOptions(context.Background(), q, store, registry, now, RecoverOptions{
		MaxEnvelopes: 2,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if report.Loaded != 2 {
		t.Fatalf("got loaded=%d, want 2", report.Loaded)
	}
	if report.ScheduledNow != 2 {
		t.Fatalf("got scheduled_now=%d, want 2", report.ScheduledNow)
	}
}

func TestStartRecoveryLoop_ReportsErrors(t *testing.T) {
	store := &recoverStore{
		envs: []Envelope{{ID: "1", Type: "missing"}},
	}
	registry := NewEnvelopeRegistry()
	q := NewQueue(1, 1, 10)

	ctx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	errCh := make(chan error, 1)
	cancel, err := StartRecoveryLoop(ctx, 20*time.Millisecond, q, store, registry, RecoverOptions{}, func(err error) {
		select {
		case errCh <- err:
		default:
		}
	})
	if err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}
	defer cancel()

	select {
	case got := <-errCh:
		if got == nil {
			t.Fatal("got nil error, want non-nil")
		}
		if got.Error() == "" {
			t.Fatal("got empty error message")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for recovery loop error")
	}
}

func TestStartRecoveryLoop_InvalidInterval(t *testing.T) {
	q := NewQueue(1, 1, 10)
	store := &recoverStore{}
	registry := NewEnvelopeRegistry()

	cancel, err := StartRecoveryLoop(context.Background(), 0, q, store, registry, RecoverOptions{}, nil)
	if err == nil {
		t.Fatal("expected invalid interval error")
	}
	if cancel != nil {
		t.Fatal("got non-nil cancel on invalid interval")
	}
}

func TestRecoverEnvelopeByID_ReenqueuesSingleJob(t *testing.T) {
	now := time.Now()
	store := &recoverStore{
		envs: []Envelope{{ID: "1", Type: "email", Payload: []byte("alpha")}},
	}
	registry := NewEnvelopeRegistry()
	var executed atomic.Int32
	registry.Register("email", func(env Envelope) (Job, error) {
		return func(ctx context.Context) error {
			executed.Add(1)
			return nil
		}, nil
	})

	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	scheduled, err := RecoverEnvelopeByID(context.Background(), q, store, registry, "1", now)
	if err != nil {
		t.Fatalf("recover by id error: %v", err)
	}
	if !scheduled {
		t.Fatal("expected scheduled=true")
	}

	time.Sleep(30 * time.Millisecond)
	if got := executed.Load(); got != 1 {
		t.Fatalf("got executed=%d, want 1", got)
	}
}

func TestRecoverEnvelopeByID_NotFound(t *testing.T) {
	store := &recoverStore{
		envs: []Envelope{{ID: "1", Type: "email"}},
	}
	registry := NewEnvelopeRegistry()
	q := NewQueue(1, 1, 10)

	_, err := RecoverEnvelopeByID(context.Background(), q, store, registry, "missing", time.Now())
	if !errors.Is(err, ErrEnvelopeNotFound) {
		t.Fatalf("got err=%v, want %v", err, ErrEnvelopeNotFound)
	}
}

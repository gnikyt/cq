package cq

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// failedSetStore is a store that supports nacked envelope listing.
type failedSetStore struct {
	*recoverStore
}

// Nacked implements the NackedEnvelopeLister interface.
func (s *failedSetStore) Nacked(ctx context.Context, query NackedEnvelopeQuery) (NackedEnvelopePage, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	out := make([]Envelope, 0, len(s.envs))
	for _, env := range s.envs {
		if env.Status != EnvelopeStatusNacked {
			continue
		}
		if query.Type != "" && env.Type != query.Type {
			continue
		}
		out = append(out, env)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	start := 0
	if query.Cursor != "" {
		for i, env := range out {
			if env.ID == query.Cursor {
				start = i + 1
				break
			}
		}
	}
	if start >= len(out) {
		return NackedEnvelopePage{Envelopes: []Envelope{}}, nil
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 50
	}
	end := min(start+limit, len(out))

	page := NackedEnvelopePage{
		Envelopes: out[start:end],
	}
	if end < len(out) && end > 0 {
		page.NextCursor = out[end-1].ID
	}
	return page, nil
}

func TestListNackedEnvelopes_Unsupported(t *testing.T) {
	store := &recoverStore{}
	_, err := ListNackedEnvelopes(context.Background(), store, NackedEnvelopeQuery{})
	if !errors.Is(err, ErrNackedListUnsupported) {
		t.Fatalf("got err=%v, want %v", err, ErrNackedListUnsupported)
	}
}

func TestListNackedEnvelopes(t *testing.T) {
	store := &failedSetStore{
		recoverStore: &recoverStore{
			envs: []Envelope{
				{ID: "1", Type: "email", Status: EnvelopeStatusNacked},
				{ID: "2", Type: "sms", Status: EnvelopeStatusNacked},
				{ID: "3", Type: "email", Status: EnvelopeStatusAcked},
			},
		},
	}

	page, err := ListNackedEnvelopes(context.Background(), store, NackedEnvelopeQuery{Type: "email"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	envs := page.Envelopes
	if len(envs) != 1 || envs[0].ID != "1" {
		t.Fatalf("got envs=%v, want only id=1", envs)
	}
}

func TestListNackedEnvelopes_CursorPagination(t *testing.T) {
	store := &failedSetStore{
		recoverStore: &recoverStore{
			envs: []Envelope{
				{ID: "1", Type: "email", Status: EnvelopeStatusNacked},
				{ID: "2", Type: "email", Status: EnvelopeStatusNacked},
				{ID: "3", Type: "email", Status: EnvelopeStatusNacked},
			},
		},
	}

	page1, err := ListNackedEnvelopes(context.Background(), store, NackedEnvelopeQuery{
		Type:  "email",
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(page1.Envelopes) != 2 {
		t.Fatalf("got page1=%d items, want 2", len(page1.Envelopes))
	}
	if page1.NextCursor == "" {
		t.Fatal("expected non-empty next cursor for first page")
	}

	page2, err := ListNackedEnvelopes(context.Background(), store, NackedEnvelopeQuery{
		Type:   "email",
		Limit:  2,
		Cursor: page1.NextCursor,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(page2.Envelopes) != 1 {
		t.Fatalf("got page2=%d items, want 1", len(page2.Envelopes))
	}
	if page2.Envelopes[0].ID != "3" {
		t.Fatalf("got page2 first id=%s, want 3", page2.Envelopes[0].ID)
	}
	if page2.NextCursor != "" {
		t.Fatalf("got page2 next cursor=%q, want empty", page2.NextCursor)
	}
}

func TestRetryNackedEnvelopeByID(t *testing.T) {
	retryAt := time.Now()
	store := &retryFailedStore{
		envs: map[string]Envelope{
			"1": {ID: "1", Type: "email", Payload: []byte("x"), Status: EnvelopeStatusNacked},
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

	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()
	defer q.Stop(true)

	ok, err := RetryNackedEnvelopeByID(context.Background(), q, store, registry, "1", retryAt)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	time.Sleep(50 * time.Millisecond)
	if got := executed.Load(); got != 1 {
		t.Fatalf("got executed=%d, want 1", got)
	}
	if got := store.retried.Load(); got != 1 {
		t.Fatalf("got retried=%d, want 1", got)
	}
	if got := store.rescheduled.Load(); got != 0 {
		t.Fatalf("got rescheduled=%d, want 0", got)
	}
}

func TestRetryNackedEnvelopeByID_FutureTimeDelaysEnqueue(t *testing.T) {
	retryAt := time.Now().Add(80 * time.Millisecond)
	store := &retryFailedStore{
		envs: map[string]Envelope{
			"1": {ID: "1", Type: "email", Payload: []byte("x"), Status: EnvelopeStatusNacked},
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

	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()
	defer q.Stop(true)

	ok, err := RetryNackedEnvelopeByID(context.Background(), q, store, registry, "1", retryAt)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	time.Sleep(20 * time.Millisecond)
	if got := executed.Load(); got != 0 {
		t.Fatalf("got executed=%d before retryAt, want 0", got)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for {
		if executed.Load() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for delayed retry")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := store.rescheduled.Load(); got != 1 {
		t.Fatalf("got rescheduled=%d, want 1", got)
	}
	if got := store.retried.Load(); got != 0 {
		t.Fatalf("got retried=%d, want 0", got)
	}
}

func TestRetryNackedEnvelopeByID_NotNacked(t *testing.T) {
	store := &recoverStore{
		envs: []Envelope{
			{ID: "1", Type: "email", Status: EnvelopeStatusEnqueued},
		},
	}
	registry := NewEnvelopeRegistry()
	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	_, err := RetryNackedEnvelopeByID(context.Background(), q, store, registry, "1", time.Now())
	if !errors.Is(err, ErrEnvelopeNotNacked) {
		t.Fatalf("got err=%v, want %v", err, ErrEnvelopeNotNacked)
	}
}

func TestDiscardNackedEnvelopeByID(t *testing.T) {
	store := &discardAwareStore{
		envs: map[string]Envelope{
			"1": {ID: "1", Status: EnvelopeStatusNacked},
		},
	}

	err := DiscardNackedEnvelopeByID(context.Background(), store, "1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !store.acked["1"] {
		t.Fatal("expected Ack to be called")
	}
}

type discardAwareStore struct {
	mu    sync.Mutex
	envs  map[string]Envelope
	acked map[string]bool
}

type retryFailedStore struct {
	mu          sync.Mutex
	envs        map[string]Envelope
	rescheduled atomic.Int32
	retried     atomic.Int32
}

func (s *retryFailedStore) Enqueue(ctx context.Context, env Envelope) error         { return nil }
func (s *retryFailedStore) Claim(ctx context.Context, id string) error              { return nil }
func (s *retryFailedStore) Ack(ctx context.Context, id string) error                { return nil }
func (s *retryFailedStore) Nack(ctx context.Context, id string, reason error) error { return nil }
func (s *retryFailedStore) Recoverable(ctx context.Context, now time.Time) ([]Envelope, error) {
	return nil, nil
}
func (s *retryFailedStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	return nil
}
func (s *retryFailedStore) RecoverByID(ctx context.Context, id string) (Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	env, ok := s.envs[id]
	if !ok {
		return Envelope{}, ErrEnvelopeNotFound
	}
	return env, nil
}
func (s *retryFailedStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	s.rescheduled.Add(1)
	return nil
}
func (s *retryFailedStore) MarkRetried(ctx context.Context, id string, retriedAt time.Time, reason string) error {
	s.retried.Add(1)
	return nil
}

func (s *discardAwareStore) Enqueue(ctx context.Context, env Envelope) error         { return nil }
func (s *discardAwareStore) Claim(ctx context.Context, id string) error              { return nil }
func (s *discardAwareStore) Nack(ctx context.Context, id string, reason error) error { return nil }
func (s *discardAwareStore) Recoverable(ctx context.Context, now time.Time) ([]Envelope, error) {
	return nil, nil
}
func (s *discardAwareStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return nil
}
func (s *discardAwareStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	return nil
}
func (s *discardAwareStore) RecoverByID(ctx context.Context, id string) (Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	env, ok := s.envs[id]
	if !ok {
		return Envelope{}, ErrEnvelopeNotFound
	}
	return env, nil
}
func (s *discardAwareStore) Ack(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acked == nil {
		s.acked = map[string]bool{}
	}
	s.acked[id] = true
	return nil
}

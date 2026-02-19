package cq

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

type typedEnvelopePayload struct {
	OrderID        string `json:"order_id"`
	SourceLocation string `json:"source_location"`
}

func TestEnvelopeJSONCodec_RoundTrip(t *testing.T) {
	codec := EnvelopeJSONCodec[typedEnvelopePayload]()
	in := typedEnvelopePayload{
		OrderID:        "ord_123",
		SourceLocation: "warehouse_a",
	}

	payload, err := codec.Marshal(in)
	if err != nil {
		t.Fatalf("marshal err: %v", err)
	}

	var out typedEnvelopePayload
	if err := codec.Unmarshal(payload, &out); err != nil {
		t.Fatalf("unmarshal err: %v", err)
	}

	if out != in {
		t.Fatalf("got payload=%+v, want %+v", out, in)
	}
}

func TestWithEnvelope_MarshalErrorPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = WithEnvelope(
		EnvelopeHandler[typedEnvelopePayload]{
			Type: "order_create",
			Codec: EnvelopeCodec[typedEnvelopePayload]{
				Marshal: func(v typedEnvelopePayload) ([]byte, error) {
					return nil, context.Canceled
				},
			},
			Handler: func(ctx context.Context, payload typedEnvelopePayload) error { return nil },
		},
		typedEnvelopePayload{OrderID: "ord_123", SourceLocation: "warehouse_a"},
	)
}

func TestWithEnvelope_RunsHandler(t *testing.T) {
	ran := false
	input := typedEnvelopePayload{
		OrderID:        "ord_123",
		SourceLocation: "warehouse_a",
	}
	job := WithEnvelope(
		EnvelopeHandler[typedEnvelopePayload]{
			Type:    "order_create",
			Codec:   EnvelopeJSONCodec[typedEnvelopePayload](),
			Handler: func(ctx context.Context, payload typedEnvelopePayload) error { ran = true; return nil },
		},
		input,
	)

	if err := job(context.Background()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ran {
		t.Fatal("expected wrapped job to run")
	}
}

func TestWithEnvelope_BuildsJobFromHandlerAndPayload(t *testing.T) {
	input := typedEnvelopePayload{
		OrderID:        "ord_123",
		SourceLocation: "warehouse_a",
	}
	var got typedEnvelopePayload
	job := WithEnvelope(
		EnvelopeHandler[typedEnvelopePayload]{
			Type:  "order_create",
			Codec: EnvelopeJSONCodec[typedEnvelopePayload](),
			Handler: func(ctx context.Context, payload typedEnvelopePayload) error {
				got = payload
				return nil
			},
		},
		input,
	)

	if err := job(context.Background()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != input {
		t.Fatalf("got payload=%+v, want %+v", got, input)
	}
}

func TestRegisterEnvelopeHandler_DecodeAndRun(t *testing.T) {
	registry := NewEnvelopeRegistry()
	var got typedEnvelopePayload
	RegisterEnvelopeHandler(registry, EnvelopeHandler[typedEnvelopePayload]{
		Type:  "order_create",
		Codec: EnvelopeJSONCodec[typedEnvelopePayload](),
		Handler: func(ctx context.Context, payload typedEnvelopePayload) error {
			got = payload
			return nil
		},
	})

	factory, ok := registry.FactoryFor("order_create")
	if !ok {
		t.Fatal("expected factory to be registered")
	}

	expected := typedEnvelopePayload{
		OrderID:        "ord_123",
		SourceLocation: "warehouse_a",
	}
	raw, err := json.Marshal(expected)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	job, err := factory(Envelope{ID: "1", Type: "order_create", Payload: raw})
	if err != nil {
		t.Fatalf("factory err: %v", err)
	}
	if err := job(context.Background()); err != nil {
		t.Fatalf("job err: %v", err)
	}
	if got != expected {
		t.Fatalf("got payload=%+v, want %+v", got, expected)
	}
}

func TestRegisterEnvelopeHandler_DecodeError(t *testing.T) {
	registry := NewEnvelopeRegistry()
	RegisterEnvelopeHandler(registry, EnvelopeHandler[typedEnvelopePayload]{
		Type:    "order_create",
		Codec:   EnvelopeJSONCodec[typedEnvelopePayload](),
		Handler: func(ctx context.Context, payload typedEnvelopePayload) error { return nil },
	})

	factory, ok := registry.FactoryFor("order_create")
	if !ok {
		t.Fatal("expected factory to be registered")
	}

	_, err := factory(Envelope{ID: "1", Type: "order_create", Payload: []byte("{")})
	if err == nil {
		t.Fatal("expected decode error")
	}
}

func TestRegisterEnvelopeHandler_NilHandlerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	RegisterEnvelopeHandler(NewEnvelopeRegistry(), EnvelopeHandler[typedEnvelopePayload]{
		Type:  "order_create",
		Codec: EnvelopeJSONCodec[typedEnvelopePayload](),
	})
}

func TestRegisterEnvelopeHandler_NilUnmarshalPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	RegisterEnvelopeHandler(
		NewEnvelopeRegistry(),
		EnvelopeHandler[typedEnvelopePayload]{
			Type: "order_create",
			Codec: EnvelopeCodec[typedEnvelopePayload]{
				Marshal: func(v typedEnvelopePayload) ([]byte, error) {
					return json.Marshal(v)
				},
			},
			Handler: func(ctx context.Context, payload typedEnvelopePayload) error { return nil },
		},
	)
}

func TestWithEnvelope_NilHandlerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	_ = WithEnvelope(EnvelopeHandler[typedEnvelopePayload]{
		Type:  "order_create",
		Codec: EnvelopeJSONCodec[typedEnvelopePayload](),
	}, typedEnvelopePayload{OrderID: "ord_123", SourceLocation: "warehouse_a"})
}

func TestEnqueueEnvelope_PersistsTypeAndPayloadAtEnqueueTime(t *testing.T) {
	store := newPayloadReplayStore()
	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()
	defer q.Stop(true)

	handler := EnvelopeHandler[typedEnvelopePayload]{
		Type:  "order_create",
		Codec: EnvelopeJSONCodec[typedEnvelopePayload](),
		Handler: func(ctx context.Context, payload typedEnvelopePayload) error {
			return nil
		},
	}
	payload := typedEnvelopePayload{OrderID: "ord_123", SourceLocation: "warehouse_a"}
	if err := EnqueueEnvelope(q, handler, payload); err != nil {
		t.Fatalf("enqueue envelope: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		store.mut.Lock()
		ready := len(store.order) == 1 && len(store.envs[store.order[0]].Payload) > 0 && store.envs[store.order[0]].Type != ""
		var env Envelope
		if len(store.order) == 1 {
			env = store.envs[store.order[0]]
		}
		store.mut.Unlock()

		if ready {
			if env.Type != "order_create" {
				t.Fatalf("got type=%q, want %q", env.Type, "order_create")
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}
			if !bytes.Equal(env.Payload, raw) {
				t.Fatalf("got payload=%q, want %q", string(env.Payload), string(raw))
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for envelope metadata persistence")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestEnqueueEnvelopeBatch_PersistsTypeAndPayloadAtEnqueueTime(t *testing.T) {
	store := newPayloadReplayStore()
	q := NewQueue(1, 1, 10, WithEnvelopeStore(store))
	q.Start()
	defer q.Stop(true)

	handler := EnvelopeHandler[typedEnvelopePayload]{
		Type:  "order_create",
		Codec: EnvelopeJSONCodec[typedEnvelopePayload](),
		Handler: func(ctx context.Context, payload typedEnvelopePayload) error {
			return nil
		},
	}
	payloads := []typedEnvelopePayload{
		{OrderID: "ord_1", SourceLocation: "warehouse_a"},
		{OrderID: "ord_2", SourceLocation: "warehouse_b"},
	}
	if err := EnqueueEnvelopeBatch(q, handler, payloads); err != nil {
		t.Fatalf("enqueue envelope batch: %v", err)
	}

	expected := map[string]int{}
	for _, payload := range payloads {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		expected[string(raw)]++
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		store.mut.Lock()
		ready := len(store.order) == len(payloads)
		gotPayloads := make([]Envelope, 0, len(store.order))
		for _, id := range store.order {
			gotPayloads = append(gotPayloads, store.envs[id])
		}
		store.mut.Unlock()

		if ready {
			for _, env := range gotPayloads {
				if env.Type != "order_create" {
					t.Fatalf("got type=%q, want %q", env.Type, "order_create")
				}
				key := string(env.Payload)
				if expected[key] == 0 {
					t.Fatalf("unexpected payload=%q", key)
				}
				expected[key]--
			}
			for key, count := range expected {
				if count != 0 {
					t.Fatalf("missing payload=%q, count=%d", key, count)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for envelope batch metadata persistence")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestEnqueueEnvelopeBatch_PreValidationPreventsPartialEnqueue(t *testing.T) {
	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	handler := EnvelopeHandler[typedEnvelopePayload]{
		Type: "order_create",
		Codec: EnvelopeCodec[typedEnvelopePayload]{
			Marshal: func(v typedEnvelopePayload) ([]byte, error) {
				if v.OrderID == "bad" {
					return nil, context.Canceled
				}
				return json.Marshal(v)
			},
		},
		Handler: func(ctx context.Context, payload typedEnvelopePayload) error { return nil },
	}

	err := EnqueueEnvelopeBatch(q, handler, []typedEnvelopePayload{
		{OrderID: "bad", SourceLocation: "warehouse_a"},
		{OrderID: "ok", SourceLocation: "warehouse_b"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if created := q.TallyOf(JobStateCreated); created != 0 {
		t.Fatalf("got created=%d, want 0", created)
	}
}

func TestDelayEnqueueEnvelopeBatch_DelaysExecution(t *testing.T) {
	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	var executed atomic.Int32
	handler := EnvelopeHandler[typedEnvelopePayload]{
		Type:  "order_create",
		Codec: EnvelopeJSONCodec[typedEnvelopePayload](),
		Handler: func(ctx context.Context, payload typedEnvelopePayload) error {
			executed.Add(1)
			return nil
		},
	}
	payloads := []typedEnvelopePayload{
		{OrderID: "ord_1", SourceLocation: "warehouse_a"},
		{OrderID: "ord_2", SourceLocation: "warehouse_b"},
	}

	delay := 150 * time.Millisecond
	if err := DelayEnqueueEnvelopeBatch(q, handler, payloads, delay); err != nil {
		t.Fatalf("delay enqueue envelope batch: %v", err)
	}

	if created := q.TallyOf(JobStateCreated); created != 0 {
		t.Fatalf("got created=%d, want 0 before delay", created)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if executed.Load() == int32(len(payloads)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for delayed envelope batch execution")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTryEnqueueEnvelopeBatch_PartialWhenQueueIsFull(t *testing.T) {
	// No workers + capacity 1 ensures first enqueue fits and second is rejected.
	q := NewQueue(0, 0, 1)
	q.Start()
	defer q.Stop(false)

	handler := EnvelopeHandler[typedEnvelopePayload]{
		Type:  "order_create",
		Codec: EnvelopeJSONCodec[typedEnvelopePayload](),
		Handler: func(ctx context.Context, payload typedEnvelopePayload) error {
			return nil
		},
	}
	payloads := []typedEnvelopePayload{
		{OrderID: "ord_1", SourceLocation: "warehouse_a"},
		{OrderID: "ord_2", SourceLocation: "warehouse_b"},
	}

	accepted, err := TryEnqueueEnvelopeBatch(q, handler, payloads)
	if err != nil {
		t.Fatalf("try enqueue envelope batch: unexpected err: %v", err)
	}
	if accepted != 1 {
		t.Fatalf("got accepted=%d, want 1", accepted)
	}
}

func TestTryEnqueueEnvelopeBatch_PreValidationError(t *testing.T) {
	q := NewQueue(1, 1, 10)
	q.Start()
	defer q.Stop(true)

	handler := EnvelopeHandler[typedEnvelopePayload]{
		Type: "order_create",
		Codec: EnvelopeCodec[typedEnvelopePayload]{
			Marshal: func(v typedEnvelopePayload) ([]byte, error) {
				if v.OrderID == "bad" {
					return nil, context.Canceled
				}
				return json.Marshal(v)
			},
		},
		Handler: func(ctx context.Context, payload typedEnvelopePayload) error { return nil },
	}

	accepted, err := TryEnqueueEnvelopeBatch(q, handler, []typedEnvelopePayload{
		{OrderID: "bad", SourceLocation: "warehouse_a"},
		{OrderID: "ok", SourceLocation: "warehouse_b"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if accepted != 0 {
		t.Fatalf("got accepted=%d, want 0", accepted)
	}
	if created := q.TallyOf(JobStateCreated); created != 0 {
		t.Fatalf("got created=%d, want 0", created)
	}
}

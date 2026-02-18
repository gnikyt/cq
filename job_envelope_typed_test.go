package cq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
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

func TestWithEnvelope_MarshalError(t *testing.T) {
	expected := errors.New("encode failed")
	ran := false

	job := WithEnvelope(
		"order_create",
		typedEnvelopePayload{OrderID: "ord_123", SourceLocation: "warehouse_a"},
		EnvelopeCodec[typedEnvelopePayload]{
			Marshal: func(v typedEnvelopePayload) ([]byte, error) {
				return nil, expected
			},
		},
		func(ctx context.Context, payload typedEnvelopePayload) error {
			ran = true
			return nil
		},
	)

	err := job(context.Background())
	if !errors.Is(err, expected) {
		t.Fatalf("got err=%v, want %v", err, expected)
	}
	if ran {
		t.Fatal("expected wrapped job not to run on marshal error")
	}
}

func TestWithEnvelope_SetsPayloadAndRuns(t *testing.T) {
	var (
		gotType    string
		gotPayload []byte
		calls      int
		ran        bool
	)
	ctx := contextWithEnvelopePayloadSetter(context.Background(), func(typ string, payload []byte) bool {
		calls++
		gotType = typ
		gotPayload = append([]byte(nil), payload...)
		return true
	})

	input := typedEnvelopePayload{
		OrderID:        "ord_123",
		SourceLocation: "warehouse_a",
	}
	job := WithEnvelope(
		"order_create",
		input,
		EnvelopeJSONCodec[typedEnvelopePayload](),
		func(ctx context.Context, payload typedEnvelopePayload) error {
			ran = true
			return nil
		},
	)

	if err := job(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ran {
		t.Fatal("expected wrapped job to run")
	}
	if calls != 1 {
		t.Fatalf("got calls=%d, want 1", calls)
	}
	if gotType != "order_create" {
		t.Fatalf("got type=%q, want %q", gotType, "order_create")
	}

	expectedPayload, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal expected payload: %v", err)
	}
	if !bytes.Equal(gotPayload, expectedPayload) {
		t.Fatalf("got payload=%q, want %q", string(gotPayload), string(expectedPayload))
	}
}

func TestWithEnvelope_BuildsJobFromHandlerAndPayload(t *testing.T) {
	input := typedEnvelopePayload{
		OrderID:        "ord_123",
		SourceLocation: "warehouse_a",
	}
	var got typedEnvelopePayload
	job := WithEnvelope(
		"order_create",
		input,
		EnvelopeJSONCodec[typedEnvelopePayload](),
		func(ctx context.Context, payload typedEnvelopePayload) error {
			got = payload
			return nil
		},
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
	codec := EnvelopeJSONCodec[typedEnvelopePayload]()

	var got typedEnvelopePayload
	RegisterEnvelopeHandler(registry, "order_create", codec, func(ctx context.Context, payload typedEnvelopePayload) error {
		got = payload
		return nil
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
	RegisterEnvelopeHandler(registry, "order_create", EnvelopeJSONCodec[typedEnvelopePayload](), func(ctx context.Context, payload typedEnvelopePayload) error {
		return nil
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

	RegisterEnvelopeHandler(NewEnvelopeRegistry(), "order_create", EnvelopeJSONCodec[typedEnvelopePayload](), nil)
}

func TestRegisterEnvelopeHandler_NilUnmarshalPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	RegisterEnvelopeHandler(
		NewEnvelopeRegistry(),
		"order_create",
		EnvelopeCodec[typedEnvelopePayload]{
			Marshal: func(v typedEnvelopePayload) ([]byte, error) {
				return json.Marshal(v)
			},
		},
		func(ctx context.Context, payload typedEnvelopePayload) error {
			return nil
		},
	)
}

func TestWithEnvelope_NilHandlerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	_ = WithEnvelope(
		"order_create",
		typedEnvelopePayload{OrderID: "ord_123", SourceLocation: "warehouse_a"},
		EnvelopeJSONCodec[typedEnvelopePayload](),
		nil,
	)
}

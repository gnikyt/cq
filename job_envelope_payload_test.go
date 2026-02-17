package cq

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestWithEnvelopePayload_BuilderError(t *testing.T) {
	expected := errors.New("encode failed")
	ran := false
	job := WithEnvelopePayload(func(ctx context.Context) error {
		ran = true
		return nil
	}, "email", func(context.Context) ([]byte, error) {
		return nil, expected
	})

	err := job(context.Background())
	if !errors.Is(err, expected) {
		t.Fatalf("got err=%v, want %v", err, expected)
	}
	if ran {
		t.Fatal("expected wrapped job not to run on builder error")
	}
}

func TestWithEnvelopePayload_SetsPayloadAndRuns(t *testing.T) {
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

	job := WithEnvelopePayload(func(ctx context.Context) error {
		ran = true
		return nil
	}, "email", func(context.Context) ([]byte, error) {
		return []byte("alpha"), nil
	})

	if err := job(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ran {
		t.Fatal("expected wrapped job to run")
	}
	if calls != 1 {
		t.Fatalf("got calls=%d, want 1", calls)
	}
	if gotType != "email" {
		t.Fatalf("got type=%q, want %q", gotType, "email")
	}
	if !bytes.Equal(gotPayload, []byte("alpha")) {
		t.Fatalf("got payload=%q, want %q", string(gotPayload), "alpha")
	}
}

func TestWithEnvelopePayload_NilBuilderPassThrough(t *testing.T) {
	ran := false
	job := WithEnvelopePayload(func(ctx context.Context) error {
		ran = true
		return nil
	}, "email", nil)

	if err := job(context.Background()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ran {
		t.Fatal("expected wrapped job to run")
	}
}

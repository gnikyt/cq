package cq

import (
	"context"
	"encoding/json"
	"fmt"
)

// EnvelopeCodec encodes and decodes a typed payload for envelope persistence/recovery.
type EnvelopeCodec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

// EnvelopeJSONCodec builds a JSON-based envelope codec for a typed payload.
func EnvelopeJSONCodec[T any]() EnvelopeCodec[T] {
	return EnvelopeCodec[T]{
		Marshal: func(v T) ([]byte, error) {
			return json.Marshal(v)
		},
		Unmarshal: func(payload []byte, out *T) error {
			return json.Unmarshal(payload, out)
		},
	}
}

// WithEnvelope builds a replay-ready job from a typed handler + payload.
func WithEnvelope[T any](typ string, payload T, codec EnvelopeCodec[T], handler func(context.Context, T) error) Job {
	if handler == nil {
		panic("cq: envelope handler must not be nil")
	}

	return WithEnvelopePayload(
		func(ctx context.Context) error {
			return handler(ctx, payload)
		},
		typ,
		func(context.Context) ([]byte, error) {
			if codec.Marshal == nil {
				return nil, fmt.Errorf("cq: envelope codec marshal must not be nil")
			}
			return codec.Marshal(payload)
		},
	)
}

// RegisterEnvelopeHandler registers a typed replay handler for envelope recovery.
// Panics when registry is nil, typ is empty, handler is nil, or codec.Unmarshal is nil.
func RegisterEnvelopeHandler[T any](
	registry *EnvelopeRegistry,
	typ string,
	codec EnvelopeCodec[T],
	handler func(context.Context, T) error,
) {
	if registry == nil {
		panic("cq: envelope registry must not be nil")
	}
	if handler == nil {
		panic("cq: envelope handler must not be nil")
	}
	if codec.Unmarshal == nil {
		panic("cq: envelope codec unmarshal must not be nil")
	}

	registry.Register(typ, func(env Envelope) (Job, error) {
		var payload T
		if err := codec.Unmarshal(env.Payload, &payload); err != nil {
			return nil, err
		}
		return func(ctx context.Context) error {
			return handler(ctx, payload)
		}, nil
	})
}

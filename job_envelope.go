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

// EnvelopeHandler declares a replayable job type with typed payload codec and handler.
type EnvelopeHandler[T any] struct {
	Type    string
	Codec   EnvelopeCodec[T]
	Handler func(context.Context, T) error
}

// EnvelopeJSONCodec builds a JSON-based envelope codec for a typed payload
// as JSON is the most common format for envelope payloads.
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

// WithEnvelope returns a Job from a typed envelope handler and payload.
// It panics when the handler configuration is invalid or payload encoding fails.
func WithEnvelope[T any](handler EnvelopeHandler[T], payload T) Job {
	job, _, err := envelopeJobAndMetadata(handler, payload)
	if err != nil {
		panic(err)
	}
	return job
}

// envelopeJobAndMetadata validates the handler, encodes payload bytes once,
// and returns both the runnable Job and enqueue-time envelope.
func envelopeJobAndMetadata[T any](handler EnvelopeHandler[T], payload T) (Job, *Envelope, error) {
	if handler.Type == "" {
		return nil, nil, fmt.Errorf("cq: envelope type must not be empty")
	}
	if handler.Handler == nil {
		return nil, nil, fmt.Errorf("cq: envelope handler must not be nil")
	}
	if handler.Codec.Marshal == nil {
		return nil, nil, fmt.Errorf("cq: envelope codec marshal must not be nil")
	}

	// Encode the payload for use in the envelope.
	encoded, err := handler.Codec.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("cq: marshal envelope payload (type=%s): %w", handler.Type, err)
	}

	return func(ctx context.Context) error {
			return handler.Handler(ctx, payload)
		}, &Envelope{
			Type:    handler.Type,
			Payload: append([]byte(nil), encoded...), // Copy to avoid mutation.
		}, nil
}

// RegisterEnvelopeHandler registers a typed replay handler for envelope recovery.
// Panics when registry is nil, type is empty, handler is nil, or codec.Unmarshal is nil.
func RegisterEnvelopeHandler[T any](registry *EnvelopeRegistry, handler EnvelopeHandler[T]) {
	if registry == nil {
		panic("cq: envelope registry must not be nil")
	}
	if handler.Handler == nil {
		panic("cq: envelope handler must not be nil")
	}
	if handler.Codec.Unmarshal == nil {
		panic("cq: envelope codec unmarshal must not be nil")
	}

	// Register the envelope handler with the registry.
	// The job returned utilizes the handler's codec to decode the payload
	// and call the handler function.
	registry.Register(handler.Type, func(env Envelope) (Job, error) {
		var payload T
		if err := handler.Codec.Unmarshal(env.Payload, &payload); err != nil {
			return nil, err
		}
		return func(ctx context.Context) error {
			return handler.Handler(ctx, payload)
		}, nil
	})
}

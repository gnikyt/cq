package cq

import "context"

// EnvelopePayloadBuilder builds payload bytes for replay metadata.
type EnvelopePayloadBuilder func(context.Context) ([]byte, error)

// WithEnvelopePayload sets replay envelope metadata (type + payload) before running a job.
// If no payload builder is configured, payload persistence is skipped and the job still runs.
func WithEnvelopePayload(job Job, typ string, payloadBuilder EnvelopePayloadBuilder) Job {
	if payloadBuilder == nil {
		return job
	}

	return func(ctx context.Context) error {
		payload, err := payloadBuilder(ctx)
		if err != nil {
			return err
		}

		_ = SetEnvelopePayload(ctx, typ, payload)
		return job(ctx)
	}
}

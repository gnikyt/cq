package cq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrNackedListUnsupported is returned when a store does not expose nacked-envelope listing.
	ErrNackedListUnsupported = errors.New("nacked envelope listing is not supported by this envelope store")
	// ErrEnvelopeNotNacked is returned when targeting an envelope that is not nacked.
	ErrEnvelopeNotNacked = errors.New("envelope is not nacked")
)

// NackedEnvelopeQuery configures nacked-envelope listing.
type NackedEnvelopeQuery struct {
	Limit  int
	Type   string
	Cursor string
}

// NackedEnvelopePage is a cursor-paginated nacked-envelope page.
type NackedEnvelopePage struct {
	Envelopes  []Envelope
	NextCursor string
}

// NackedEnvelopeLister is an optional extension for EnvelopeStore implementations
// that expose nacked-envelope listing.
type NackedEnvelopeLister interface {
	Nacked(ctx context.Context, query NackedEnvelopeQuery) (NackedEnvelopePage, error)
}

// EnvelopeRetryStateStore is an optional extension for EnvelopeStore implementations
// that want explicit lifecycle transitions for manual retries.
type EnvelopeRetryStateStore interface {
	MarkRetried(ctx context.Context, id string, retriedAt time.Time, reason string) error
}

// ListNackedEnvelopes returns nacked envelopes from stores that support it.
func ListNackedEnvelopes(ctx context.Context, store EnvelopeStore, query NackedEnvelopeQuery) (NackedEnvelopePage, error) {
	lister, ok := store.(NackedEnvelopeLister)
	if !ok {
		return NackedEnvelopePage{}, ErrNackedListUnsupported
	}
	return lister.Nacked(ctx, query)
}

// RetryNackedEnvelopeByID retries a nacked envelope by ID.
// If retryAt is in the future, the retry is delayed until that time.
// Otherwise, retry is enqueued immediately.
func RetryNackedEnvelopeByID(
	ctx context.Context,
	queue *Queue,
	store EnvelopeStore,
	registry *EnvelopeRegistry,
	id string,
	retryAt time.Time,
) (bool, error) {
	if queue == nil {
		return false, fmt.Errorf("nacked ops: queue is required")
	}
	if store == nil {
		return false, fmt.Errorf("nacked ops: envelope store is required")
	}
	if registry == nil {
		return false, fmt.Errorf("nacked ops: envelope registry is required")
	}
	if id == "" {
		return false, fmt.Errorf("nacked ops: envelope id is required")
	}

	env, err := store.RecoverByID(ctx, id)
	if err != nil {
		return false, err
	}
	if env.Status != EnvelopeStatusNacked {
		return false, ErrEnvelopeNotNacked
	}

	factory, ok := registry.FactoryFor(env.Type)
	if !ok {
		return false, fmt.Errorf("nacked ops: no factory registered for envelope (type=%s)", env.Type)
	}
	job, buildErr := factory(env)
	if buildErr != nil {
		return false, fmt.Errorf("nacked ops: build envelope (id=%s, type=%s): %w", env.ID, env.Type, buildErr)
	}

	envelope := &Envelope{
		ID:      env.ID,
		Type:    env.Type,
		Payload: append([]byte(nil), env.Payload...),
	}

	effectiveRetryAt := retryAt
	now := time.Now()
	if effectiveRetryAt.IsZero() {
		effectiveRetryAt = now
	}

	if effectiveRetryAt.After(now) {
		// Schedule delayed retry.
		queue.doDelayEnqueue(job, effectiveRetryAt.Sub(now), enqueueOptions{
			blocking: true,
			envelope: envelope,
			id:       env.ID,
		})
	} else {
		// Immediate retry.
		ok, enqueueErr := queue.doEnqueueWithErr(job, enqueueOptions{
			blocking: true,
			envelope: envelope,
			id:       env.ID,
		})
		if !ok || enqueueErr != nil {
			return false, enqueueErr
		}
		effectiveRetryAt = now
	}

	if effectiveRetryAt.After(now) {
		// Delayed retry uses scheduling.
		if err := store.Reschedule(ctx, env.ID, effectiveRetryAt, EnvelopeRescheduleReasonManualRetry); err != nil {
			return false, err
		}
	} else {
		// Immediate retry... use MarkRetried when supported.
		if retryStore, ok := store.(EnvelopeRetryStateStore); ok {
			if err := retryStore.MarkRetried(ctx, env.ID, now, EnvelopeRescheduleReasonManualRetry); err != nil {
				return false, err
			}
		} else {
			// Fallback for stores that only support Reschedule.
			if err := store.Reschedule(ctx, env.ID, now, EnvelopeRescheduleReasonManualRetry); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// DiscardNackedEnvelopeByID marks a nacked envelope as acknowledged/discarded.
func DiscardNackedEnvelopeByID(ctx context.Context, store EnvelopeStore, id string) error {
	if store == nil {
		return fmt.Errorf("nacked ops: envelope store is required")
	}
	if id == "" {
		return fmt.Errorf("nacked ops: envelope id is required")
	}

	env, err := store.RecoverByID(ctx, id)
	if err != nil {
		return err
	}
	if env.Status != EnvelopeStatusNacked {
		return ErrEnvelopeNotNacked
	}
	return store.Ack(ctx, id)
}

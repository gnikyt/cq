package cq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// EnvelopeJobFactory builds a runnable Job from a persisted envelope.
type EnvelopeJobFactory func(env Envelope) (Job, error)

// EnvelopeRegistry maps envelope Type values to replay factories.
type EnvelopeRegistry struct {
	mut       sync.RWMutex
	factories map[string]EnvelopeJobFactory
}

// RecoverOptions controls recovery behavior.
type RecoverOptions struct {
	// MaxEnvelopes limits how many recovered envelopes are processed per run.
	// Zero means no limit.
	MaxEnvelopes int
	// ContinueOnError keeps processing envelopes when a factory is missing or
	// a factory returns a build error. If false, the first error stops recovery.
	ContinueOnError bool
	// OnError receives per-envelope recover/build errors when ContinueOnError is true.
	// It is not called for store or argument validation errors.
	OnError func(env Envelope, err error)
}

// RecoverReport summarizes a recovery run.
type RecoverReport struct {
	Loaded             int
	ScheduledNow       int
	ScheduledDelayed   int
	SkippedUnknownType int
	FailedBuild        int
}

// NewEnvelopeRegistry creates an empty envelope registry.
func NewEnvelopeRegistry() *EnvelopeRegistry {
	return &EnvelopeRegistry{
		factories: make(map[string]EnvelopeJobFactory),
	}
}

// Register stores a factory for a logical envelope type.
// Panics when typ is empty or factory is nil.
func (r *EnvelopeRegistry) Register(typ string, factory EnvelopeJobFactory) {
	if typ == "" {
		panic("cq: envelope type must not be empty")
	}
	if factory == nil {
		panic("cq: envelope factory must not be nil")
	}
	r.mut.Lock()
	r.factories[typ] = factory
	r.mut.Unlock()
}

// FactoryFor returns the factory for a given envelope type.
func (r *EnvelopeRegistry) FactoryFor(typ string) (EnvelopeJobFactory, bool) {
	r.mut.RLock()
	defer r.mut.RUnlock()
	f, ok := r.factories[typ]
	return f, ok
}

// RecoverEnvelopes loads recoverable envelopes from store and re-enqueues them.
// If env.NextRunAt is in the future, jobs are delayed until that time.
// Returns number of envelopes scheduled for replay.
func RecoverEnvelopes(ctx context.Context, queue *Queue, store EnvelopeStore, registry *EnvelopeRegistry, now time.Time) (int, error) {
	report, err := RecoverEnvelopesWithOptions(ctx, queue, store, registry, now, RecoverOptions{})
	return report.ScheduledNow + report.ScheduledDelayed, err
}

// RecoverEnvelopesWithOptions loads recoverable envelopes from store and
// re-enqueues them with configurable error handling and limits.
func RecoverEnvelopesWithOptions(
	ctx context.Context,
	queue *Queue,
	store EnvelopeStore,
	registry *EnvelopeRegistry,
	now time.Time,
	opts RecoverOptions,
) (RecoverReport, error) {
	if queue == nil {
		return RecoverReport{}, fmt.Errorf("recover: queue is required")
	}
	if store == nil {
		return RecoverReport{}, fmt.Errorf("recover: envelope store is required")
	}
	if registry == nil {
		return RecoverReport{}, fmt.Errorf("recover: envelope registry is required")
	}

	envs, err := store.Recoverable(ctx, now)
	if err != nil {
		return RecoverReport{}, err
	}

	if opts.MaxEnvelopes > 0 && len(envs) > opts.MaxEnvelopes {
		envs = envs[:opts.MaxEnvelopes]
	}

	report := RecoverReport{
		Loaded: len(envs),
	}

	handleEnvelopeErr := func(env Envelope, envelopeErr error) error {
		if opts.ContinueOnError {
			if opts.OnError != nil {
				opts.OnError(env, envelopeErr) // Call the error handler.
			}
			return nil // Continue processing on error.
		}
		return envelopeErr // Stop processing on first error.
	}

	for _, env := range envs {
		factory, ok := registry.FactoryFor(env.Type)
		if !ok {
			report.SkippedUnknownType++
			envelopeErr := handleEnvelopeErr(env, fmt.Errorf("recover: no factory registered for envelope (type=%s)", env.Type))
			if envelopeErr != nil {
				return report, envelopeErr
			}
			continue
		}

		job, buildErr := factory(env)
		if buildErr != nil {
			report.FailedBuild++
			envelopeErr := handleEnvelopeErr(env, fmt.Errorf("recover: build envelope (id=%s, type=%s): %w", env.ID, env.Type, buildErr))
			if envelopeErr != nil {
				return report, envelopeErr
			}
			continue
		}

		if !env.NextRunAt.IsZero() && env.NextRunAt.After(now) {
			queue.DelayEnqueue(job, env.NextRunAt.Sub(now))
			report.ScheduledDelayed++
		} else {
			queue.Enqueue(job)
			report.ScheduledNow++
		}
	}

	return report, nil
}

// StartRecoveryLoop runs recovery on an interval until context cancellation.
// It performs one recovery run immediately before waiting for the first tick.
// The returned cancel function stops the loop early.
func StartRecoveryLoop(
	ctx context.Context,
	interval time.Duration,
	queue *Queue,
	store EnvelopeStore,
	registry *EnvelopeRegistry,
	recoverOpts RecoverOptions,
	onError func(error),
) (context.CancelFunc, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("recover: interval must be > 0")
	}

	loopCtx, cancel := context.WithCancel(ctx)
	run := func() {
		_, err := RecoverEnvelopesWithOptions(loopCtx, queue, store, registry, time.Now(), recoverOpts)
		if err != nil && onError != nil {
			onError(err)
		}
	}

	go func() {
		run() // First pass.

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-loopCtx.Done():
				return // Context cancelled, stop the loop.
			case <-ticker.C:
				run() // Run the recovery loop.
			}
		}
	}()

	return cancel, nil
}

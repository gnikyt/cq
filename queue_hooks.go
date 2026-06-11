package cq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type hookName string

const (
	hookEnqueue        hookName = "enqueue"
	hookStart          hookName = "start"
	hookSuccess        hookName = "success"
	hookFailure        hookName = "failure"
	hookDiscard        hookName = "discard"
	hookReschedule     hookName = "reschedule"
	hookAttemptStart   hookName = "attempt_start"
	hookAttemptSuccess hookName = "attempt_success"
	hookAttemptFailure hookName = "attempt_failure"
)

// JobEvent is a structured queue lifecycle event for observability integrations.
type JobEvent struct {
	ID         string
	Name       string
	QueueName  string
	Attributes map[string]string

	EnqueuedAt        time.Time
	StartedAt         time.Time
	FinishedAt        time.Time
	WaitDuration      time.Duration
	ExecutionDuration time.Duration

	Attempt int
	State   JobState
	Err     error

	Delay            time.Duration
	RescheduleReason string
}

// Hooks defines observational lifecycle callbacks for queue transitions.
// Callbacks receive the relevant acceptance, execution, or reschedule context.
type Hooks struct {
	OnEnqueue        func(context.Context, JobEvent)
	OnStart          func(context.Context, JobEvent)
	OnSuccess        func(context.Context, JobEvent)
	OnFailure        func(context.Context, JobEvent)
	OnDiscard        func(context.Context, JobEvent)
	OnReschedule     func(context.Context, JobEvent)
	OnAttemptStart   func(context.Context, JobEvent)
	OnAttemptSuccess func(context.Context, JobEvent)
	OnAttemptFailure func(context.Context, JobEvent)
}

// eventFromMeta creates a JobEvent from the given metadata, state, and error.
func eventFromMeta(
	meta JobMeta,
	queueName string,
	state JobState,
	err error,
	startedAt time.Time,
	finishedAt time.Time,
) JobEvent {
	event := JobEvent{
		ID:         meta.ID,
		Name:       meta.Name,
		QueueName:  queueName,
		Attributes: cloneStringMap(meta.Attributes),
		EnqueuedAt: meta.EnqueuedAt,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		Attempt:    meta.Attempt,
		State:      state,
		Err:        err,
	}
	if !meta.EnqueuedAt.IsZero() && !startedAt.IsZero() {
		event.WaitDuration = startedAt.Sub(meta.EnqueuedAt)
	}
	if !startedAt.IsZero() && !finishedAt.IsZero() {
		event.ExecutionDuration = finishedAt.Sub(startedAt)
	}
	return event
}

// emitHook emits a hook event to the given function.
func (q *Queue) emitHook(ctx context.Context, name hookName, fn func(context.Context, JobEvent), event JobEvent) {
	if fn == nil {
		return
	}
	event.Attributes = cloneStringMap(event.Attributes)

	defer func() {
		if r := recover(); r != nil && q.panicHandler != nil {
			q.panicHandler(fmt.Errorf("queue: hook panic (name=%s): %v", string(name), r))
		}
	}()

	fn(ctx, event)
}

// dispatchEnqueue dispatches the enqueue hook event when a job is enqueued.
func (q *Queue) dispatchEnqueue(ctx context.Context, meta JobMeta) {
	event := eventFromMetaWithoutTiming(meta, q.name, JobStateCreated, nil)
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookEnqueue, hooks.OnEnqueue, event)
	}
}

// dispatchStart dispatches the start hook event when a worker starts processing a job.
func (q *Queue) dispatchStart(ctx context.Context, meta JobMeta, startedAt time.Time) {
	event := eventFromMeta(meta, q.name, JobStateActive, nil, startedAt, time.Time{})
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookStart, hooks.OnStart, event)
	}
}

// dispatchResult dispatches the result hook event when a job completes.
func (q *Queue) dispatchResult(
	ctx context.Context,
	meta JobMeta,
	err error,
	startedAt time.Time,
	finishedAt time.Time,
) {
	if err != nil {
		state := JobStateFailed
		if errors.Is(err, ErrJobCancelled) {
			state = JobStateCancelled
		}
		if errors.Is(err, ErrDiscard) || errors.Is(err, errQueueDiscardedOutcome) {
			state = JobStateDiscarded
		}
		event := eventFromMeta(meta, q.name, state, err, startedAt, finishedAt)
		if state == JobStateDiscarded {
			for _, hooks := range q.hooks {
				q.emitHook(ctx, hookDiscard, hooks.OnDiscard, event)
			}
			return // Discarded outcomes are terminal.
		}
		for _, hooks := range q.hooks {
			q.emitHook(ctx, hookFailure, hooks.OnFailure, event)
		}
		return // Failed outcomes are terminal.
	}

	event := eventFromMeta(meta, q.name, JobStateCompleted, nil, startedAt, finishedAt)
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookSuccess, hooks.OnSuccess, event)
	}
}

// dispatchReschedule dispatches the reschedule hook event when a job is rescheduled.
func (q *Queue) dispatchReschedule(ctx context.Context, meta JobMeta, delay time.Duration, reason string) {
	q.markJobRescheduled(reason)
	event := eventFromMetaWithoutTiming(meta, q.name, JobStatePending, nil)
	event.Delay = delay
	event.RescheduleReason = reason
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookReschedule, hooks.OnReschedule, event)
	}
}

// dispatchAttemptStart dispatches the attempt start hook event when a job attempt starts.
func (q *Queue) dispatchAttemptStart(ctx context.Context, meta JobMeta, startedAt time.Time) {
	event := eventFromMeta(meta, q.name, JobStateActive, nil, startedAt, time.Time{})
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookAttemptStart, hooks.OnAttemptStart, event)
	}
}

// dispatchAttemptResult dispatches the attempt result hook event when a job attempt completes.
func (q *Queue) dispatchAttemptResult(
	ctx context.Context,
	meta JobMeta,
	err error,
	startedAt time.Time,
	finishedAt time.Time,
) {
	if err != nil {
		event := eventFromMeta(meta, q.name, JobStateFailed, err, startedAt, finishedAt)
		for _, hooks := range q.hooks {
			q.emitHook(ctx, hookAttemptFailure, hooks.OnAttemptFailure, event)
		}
		return
	}
	event := eventFromMeta(meta, q.name, JobStateCompleted, nil, startedAt, finishedAt)
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookAttemptSuccess, hooks.OnAttemptSuccess, event)
	}
}

// retryAttemptEmitterKey is the context key for the retry attempt emitter.
type retryAttemptEmitterKey struct{}

// retryAttemptEmitter is a function that emits a retry attempt start or result event.
type retryAttemptEmitter struct {
	start  func(context.Context, JobMeta, time.Time)
	result func(context.Context, JobMeta, error, time.Time, time.Time)
}

// contextWithRetryAttemptEmitter returns a new context with the retry attempt emitter.
func contextWithRetryAttemptEmitter(ctx context.Context, emitter retryAttemptEmitter) context.Context {
	return context.WithValue(ctx, retryAttemptEmitterKey{}, emitter)
}

// retryAttemptEmitterFromContext returns the retry attempt emitter from the context.
func retryAttemptEmitterFromContext(ctx context.Context) (retryAttemptEmitter, bool) {
	emitter, ok := ctx.Value(retryAttemptEmitterKey{}).(retryAttemptEmitter)
	return emitter, ok
}

// eventFromMetaWithoutTiming creates a JobEvent from the given metadata, state, and error without timing information.
func eventFromMetaWithoutTiming(meta JobMeta, queueName string, state JobState, err error) JobEvent {
	var zeroTime time.Time
	return eventFromMeta(meta, queueName, state, err, zeroTime, zeroTime)
}

package cq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type hookName string

const (
	hookEnqueue    hookName = "enqueue"
	hookStart      hookName = "start"
	hookSuccess    hookName = "success"
	hookFailure    hookName = "failure"
	hookReschedule hookName = "reschedule"
)

// JobEvent is a structured queue lifecycle event for observability integrations.
type JobEvent struct {
	ID               string
	Name             string
	Attributes       map[string]string
	EnqueuedAt       time.Time
	Attempt          int
	State            JobState
	Err              error
	Delay            time.Duration
	RescheduleReason string
}

// Hooks defines observational lifecycle callbacks for queue transitions.
// Callbacks receive the relevant acceptance, execution, or reschedule context.
type Hooks struct {
	OnEnqueue    func(context.Context, JobEvent)
	OnStart      func(context.Context, JobEvent)
	OnSuccess    func(context.Context, JobEvent)
	OnFailure    func(context.Context, JobEvent)
	OnReschedule func(context.Context, JobEvent)
}

// eventFromMeta creates a JobEvent from the given metadata, state, and error.
func eventFromMeta(meta JobMeta, state JobState, err error) JobEvent {
	event := JobEvent{
		ID:         meta.ID,
		Name:       meta.Name,
		Attributes: cloneStringMap(meta.Attributes),
		EnqueuedAt: meta.EnqueuedAt,
		Attempt:    meta.Attempt,
		State:      state,
		Err:        err,
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
	event := eventFromMeta(meta, JobStateCreated, nil)
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookEnqueue, hooks.OnEnqueue, event)
	}
}

// dispatchStart dispatches the start hook event when a worker starts processing a job.
func (q *Queue) dispatchStart(ctx context.Context, meta JobMeta) {
	event := eventFromMeta(meta, JobStateActive, nil)
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookStart, hooks.OnStart, event)
	}
}

// dispatchResult dispatches the result hook event when a job completes.
func (q *Queue) dispatchResult(ctx context.Context, meta JobMeta, err error) {
	if err != nil {
		state := JobStateFailed
		if errors.Is(err, ErrJobCancelled) {
			state = JobStateCancelled
		}
		event := eventFromMeta(meta, state, err)
		for _, hooks := range q.hooks {
			q.emitHook(ctx, hookFailure, hooks.OnFailure, event)
		}
		return
	}

	event := eventFromMeta(meta, JobStateCompleted, nil)
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookSuccess, hooks.OnSuccess, event)
	}
}

// dispatchReschedule dispatches the reschedule hook event when a job is rescheduled.
func (q *Queue) dispatchReschedule(ctx context.Context, meta JobMeta, delay time.Duration, reason string) {
	event := eventFromMeta(meta, JobStatePending, nil)
	event.Delay = delay
	event.RescheduleReason = reason
	for _, hooks := range q.hooks {
		q.emitHook(ctx, hookReschedule, hooks.OnReschedule, event)
	}
}

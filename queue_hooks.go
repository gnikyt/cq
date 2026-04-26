package cq

import (
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
	ID                 string
	EnqueuedAt         time.Time
	Attempt            int
	State              JobState
	Err                error
	Delay              time.Duration
	RescheduleReason   string
}

// Hooks defines optional lifecycle callbacks for queue transitions.
type Hooks struct {
	OnEnqueue    func(JobEvent)
	OnStart      func(JobEvent)
	OnSuccess    func(JobEvent)
	OnFailure    func(JobEvent)
	OnReschedule func(JobEvent)
}

// eventFromMeta creates a JobEvent from the given metadata, state, and error.
func eventFromMeta(meta JobMeta, state JobState, err error) JobEvent {
	event := JobEvent{
		ID:         meta.ID,
		EnqueuedAt: meta.EnqueuedAt,
		Attempt:    meta.Attempt,
		State:      state,
		Err:        err,
	}
	return event
}

// emitHook emits a hook event to the given function.
func (q *Queue) emitHook(name hookName, fn func(JobEvent), event JobEvent) {
	if fn == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil && q.panicHandler != nil {
			q.panicHandler(fmt.Errorf("queue: hook panic (name=%s): %v", string(name), r))
		}
	}()

	fn(event)
}

// dispatchEnqueue dispatches the enqueue hook event when a job is enqueued.
func (q *Queue) dispatchEnqueue(meta JobMeta) {
	event := eventFromMeta(meta, JobStateCreated, nil)
	for _, hooks := range q.hooks {
		q.emitHook(hookEnqueue, hooks.OnEnqueue, event)
	}
}

// dispatchStart dispatches the start hook event when a worker starts processing a job.
func (q *Queue) dispatchStart(meta JobMeta) {
	event := eventFromMeta(meta, JobStateActive, nil)
	for _, hooks := range q.hooks {
		q.emitHook(hookStart, hooks.OnStart, event)
	}
}

// dispatchResult dispatches the result hook event when a job completes.
func (q *Queue) dispatchResult(meta JobMeta, err error) {
	if err != nil {
		event := eventFromMeta(meta, JobStateFailed, err)
		for _, hooks := range q.hooks {
			q.emitHook(hookFailure, hooks.OnFailure, event)
		}
		return
	}

	event := eventFromMeta(meta, JobStateCompleted, nil)
	for _, hooks := range q.hooks {
		q.emitHook(hookSuccess, hooks.OnSuccess, event)
	}
}

// dispatchReschedule dispatches the reschedule hook event when a job is rescheduled.
func (q *Queue) dispatchReschedule(meta JobMeta, delay time.Duration, reason string) {
	event := eventFromMeta(meta, JobStatePending, nil)
	event.Delay = delay
	event.RescheduleReason = reason
	for _, hooks := range q.hooks {
		q.emitHook(hookReschedule, hooks.OnReschedule, event)
	}
}

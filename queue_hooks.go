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
	EnvelopeType       string
	EnvelopePayloadLen int
	EnvelopePayload    []byte
}

// Hooks defines optional lifecycle callbacks for queue transitions.
type Hooks struct {
	OnEnqueue    func(JobEvent)
	OnStart      func(JobEvent)
	OnSuccess    func(JobEvent)
	OnFailure    func(JobEvent)
	OnReschedule func(JobEvent)
}

// defaultHooks returns the default hooks for the queue.
// It reports envelope lifecycle events to persistence.
func (q *Queue) defaultHooks() Hooks {
	return Hooks{
		OnEnqueue: func(event JobEvent) {
			var envelope *Envelope
			if event.EnvelopeType != "" || len(event.EnvelopePayload) > 0 {
				envelope = &Envelope{
					Type:    event.EnvelopeType,
					Payload: append([]byte(nil), event.EnvelopePayload...), // Copy payload.
				}
			}
			q.reportEnvelopeEnqueue(metaFromEvent(event), envelope)
		},
		OnStart: func(event JobEvent) {
			q.reportEnvelopeClaim(metaFromEvent(event))
		},
		OnSuccess: func(event JobEvent) {
			q.reportEnvelopeResult(metaFromEvent(event), nil)
		},
		OnFailure: func(event JobEvent) {
			q.reportEnvelopeResult(metaFromEvent(event), event.Err)
		},
		OnReschedule: func(event JobEvent) {
			q.reportEnvelopeReschedule(metaFromEvent(event), event.Delay, event.RescheduleReason)
		},
	}
}

// metaFromEvent creates a JobMeta from the given event.
func metaFromEvent(event JobEvent) JobMeta {
	return JobMeta{
		ID:         event.ID,
		EnqueuedAt: event.EnqueuedAt,
		Attempt:    event.Attempt,
	}
}

// eventFromMeta creates a JobEvent from the given metadata, state, error, and envelope.
func eventFromMeta(meta JobMeta, state JobState, err error, envelope *Envelope) JobEvent {
	event := JobEvent{
		ID:         meta.ID,
		EnqueuedAt: meta.EnqueuedAt,
		Attempt:    meta.Attempt,
		State:      state,
		Err:        err,
	}
	if envelope != nil {
		event.EnvelopeType = envelope.Type
		event.EnvelopePayload = append([]byte(nil), envelope.Payload...) // Copy payload.
		event.EnvelopePayloadLen = len(envelope.Payload)
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
func (q *Queue) dispatchEnqueue(meta JobMeta, envelope *Envelope) {
	event := eventFromMeta(meta, JobStateCreated, nil, envelope)
	for _, hooks := range q.hooks {
		q.emitHook(hookEnqueue, hooks.OnEnqueue, event)
	}
}

// dispatchStart dispatches the start hook event when a worker starts processing a job.
func (q *Queue) dispatchStart(meta JobMeta, envelope *Envelope) {
	event := eventFromMeta(meta, JobStateActive, nil, envelope)
	for _, hooks := range q.hooks {
		q.emitHook(hookStart, hooks.OnStart, event)
	}
}

// dispatchResult dispatches the result hook event when a job completes.
func (q *Queue) dispatchResult(meta JobMeta, envelope *Envelope, err error) {
	if err != nil {
		event := eventFromMeta(meta, JobStateFailed, err, envelope)
		for _, hooks := range q.hooks {
			q.emitHook(hookFailure, hooks.OnFailure, event)
		}
		return
	}

	event := eventFromMeta(meta, JobStateCompleted, nil, envelope)
	for _, hooks := range q.hooks {
		q.emitHook(hookSuccess, hooks.OnSuccess, event)
	}
}

// dispatchReschedule dispatches the reschedule hook event when a job is rescheduled.
func (q *Queue) dispatchReschedule(meta JobMeta, delay time.Duration, reason string) {
	event := eventFromMeta(meta, JobStatePending, nil, nil)
	event.Delay = delay
	event.RescheduleReason = reason
	for _, hooks := range q.hooks {
		q.emitHook(hookReschedule, hooks.OnReschedule, event)
	}
}

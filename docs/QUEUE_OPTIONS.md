# Queue Options

Queue options configure runtime behavior for worker lifecycle, context management, panic handling, envelope persistence, and job ID generation.

## Option Reference

- `cq.WithWorkerIdleTick(d)`  
  Interval for idle-worker cleanup. Default is `5*time.Second`.

- `cq.WithContext(ctx)`  
  Parent context for queue workers and jobs. Queue derives a cancel function from this context.

- `cq.WithCancelableContext(ctx, cancel)`  
  Parent context with an explicit cancel function (useful for signal-aware shutdown).

- `cq.WithPanicHandler(fn)`  
  Handles recovered panics from job execution and internal queue reporting.

- `cq.WithEnvelopeStore(store)`  
  Persists envelope lifecycle metadata (`enqueue`, `claim`, `ack`, `nack`, `reschedule`) for recovery and replay.

- `cq.WithHooks(hooks)`  
  Registers optional queue lifecycle callbacks (`OnEnqueue`, `OnStart`, `OnSuccess`, `OnFailure`, `OnReschedule`) for observability integrations. You can pass `WithHooks` multiple times; callbacks are appended and all are executed.

- `cq.WithIDGenerator(fn)`  
  Overrides fallback job ID generation. If the generator returns an empty string, the queue falls back to its atomic counter.

- `cq.WithPauseStore(store, key)`  
  Enables distributed pause/resume state using a shared store. All queue instances using the same key will honor the same pause flag.

- `cq.WithPausePollTick(d)`  
  Interval for polling distributed pause state. Default is `1*time.Second`.

- `cq.WithPauseBehavior(mode)`  
  Controls enqueue behavior while paused:
  - `cq.PauseBuffer` (default): accept enqueue and buffer until resume
  - `cq.PauseReject`: reject enqueue while paused

- `cq.WithMiddleware(mw...)`  
  Applies queue-level wrappers to every job accepted by the queue.
  Registration order is preserved: `WithMiddleware(a, b)` executes as `a(b(job))`.

## Example

```go
queue := cq.NewQueue(1, 10, 100,
	cq.WithWorkerIdleTick(500*time.Millisecond),
	cq.WithContext(ctx),
	cq.WithPanicHandler(func(err any) {
		log.Printf("panic: %v", err)
	}),
	cq.WithHooks(cq.Hooks{
		OnFailure: func(event cq.JobEvent) {
			log.Printf("job failed (id=%s): %v", event.ID, event.Err)
		},
	}),
	cq.WithIDGenerator(func() string {
		return uuid.NewString()
	}),
)

queue.Enqueue(func(ctx context.Context) error {
	meta := cq.MetaFromContext(ctx)
	log.Printf("job_id=%s", meta.ID)
	return nil
})
```

## Pause / Resume

Use `Pause()` to stop starting new work while allowing running jobs to finish:

```go
if err := queue.Pause(); err != nil {
	log.Fatal(err)
}

// Jobs can still be enqueued while paused, but workers will not start them.

if err := queue.Resume(); err != nil {
	log.Fatal(err)
}
```

## Distributed Pause Store

`PauseStore` is optional and only needed when you want multiple queue instances
to honor the same pause state.

```go
type PauseStore interface {
	IsPaused(ctx context.Context, key string) (bool, error)
	SetPaused(ctx context.Context, key string, paused bool) error
}
```

Configure it with:

```go
queue := cq.NewQueue(1, 10, 100,
	cq.WithPauseStore(store, "orders"),
	cq.WithPausePollTick(500*time.Millisecond),
	cq.WithPauseBehavior(cq.PauseBuffer),
)
```

You can implement time-window pauses by either, for example:
- Having an external scheduler call `SetPaused(key, true/false)` at start/end, or
- Making `IsPaused` compute pause state from a DB table with `start_at`/`end_at`.

### Pause Behavior Notes

When paused:
- `PauseBuffer` keeps accepting enqueue and defers execution until resume.
- `PauseReject` rejects enqueue.

- For rejection-awareness, use `TryEnqueue` (or `TryEnqueueEnvelope`) since they return if enqueue happened.
- For typed rejection reasons, use `EnqueueOrError` / `TryEnqueueOrError` and check:
`cq.ErrQueuePaused`, `cq.ErrQueueStopped`, `cq.ErrQueueFull`.

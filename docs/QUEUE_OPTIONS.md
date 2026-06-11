# Queue Options

Queue options configure runtime behavior for worker lifecycle, context management, panic handling, and job ID generation.

## Runtime Worker Range

Use `SetWorkerRange(min, max)` to change worker limits at runtime:

```go
if err := queue.SetWorkerRange(2, 20); err != nil {
	log.Fatal(err)
}
```

Behavior:
- Increasing `min` starts additional workers immediately.
- Decreasing `max` does not stop active workers, the idle cleanup drains excess workers.

## Option Reference

- `cq.WithWorkerIdleTick(d)`  
  Interval for idle-worker cleanup. Default is `5*time.Second`.

- `cq.WithContext(ctx)`  
  Parent context for queue workers and jobs. Queue derives a cancel function from this context.

- `cq.WithCancelableContext(ctx, cancel)`  
  Parent context with an explicit cancel function (useful for signal-aware shutdown).

- `cq.WithPanicHandler(fn)`  
  Handles recovered panics from job execution and internal queue reporting.

- `cq.WithHooks(hooks)`  
  Registers optional queue lifecycle callbacks (`OnEnqueue`, `OnStart`, `OnSuccess`, `OnFailure`, `OnReschedule`) for observability integrations. `OnEnqueue` receives the acceptance context; the execution hooks receive the job context; and `OnReschedule` receives the rescheduling job context. Result-hook contexts may already be cancelled. Use `context.WithoutCancel(ctx)` when reporting must outlive job cancellation. You can pass `WithHooks` multiple times. Callbacks are appended and all are executed.

- `cq.WithIDGenerator(fn)`  
  Overrides fallback job ID generation. If the generator returns an empty string, the queue falls back to its atomic counter.

- `cq.WithPauseStore(store, key)`  
  Enables distributed pause/resume state using a shared store. All queue instances using the same key will honor the same pause flag.

- `cq.WithPausePollTick(d)`  
  Interval for polling distributed pause state. Default is `1*time.Second`.

- `cq.WithPauseBehavior(mode)`  
  Controls submission behavior while paused:
  - `cq.PauseBuffer` (default): accept submissions and buffer until resume
  - `cq.PauseReject`: reject submissions while paused

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
		OnFailure: func(_ context.Context, event cq.JobEvent) {
			log.Printf("job failed (id=%s): %v", event.ID, event.Err)
		},
	}),
	cq.WithIDGenerator(func() string {
		return uuid.NewString()
	}),
)

_, _ = queue.Submit(context.Background(), func(ctx context.Context) error {
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
- `PauseBuffer` keeps accepting submissions and defers execution until resume.
- `PauseReject` rejects submissions.

- For rejection-awareness, use `Submit` to receive a handle only when accepted.
- For non-blocking submission, add `WithNonBlocking()`.
- For typed rejection reasons, check:
`cq.ErrQueuePaused`, `cq.ErrQueueStopped`, `cq.ErrQueueFull`.
- `Submit(ctx, job)` returns `ctx.Err()` if its context ends before acceptance.

## Context-Aware Shutdown

In addition to `Stop(true/false)`, you can bound graceful shutdown:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := queue.StopContext(ctx); err != nil {
	// context deadline/cancel before drain
	log.Printf("stop bounded: %v", err)
}
```

Or use convenience timeout:

```go
if err := queue.StopTimeout(5 * time.Second); err != nil {
	log.Printf("stop timeout: %v", err)
}
```

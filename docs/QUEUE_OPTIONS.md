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

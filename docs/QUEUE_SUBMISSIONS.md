# Queue Submissions

`Submit` returns a `JobHandle` after the queue accepts responsibility for a job.
The context passed to `Submit` controls waiting for acceptance only. Cancelling
that context after acceptance does not cancel the job.

```go
handle, err := queue.Submit(ctx, job)
if err != nil {
	return err
}

if err := handle.Wait(context.Background()); err != nil {
	log.Printf("job finished with error: %v", err)
}
```

`Done` supports composition with other channels, while `Wait` is the convenient
blocking form. `Result` returns metadata, timestamps, and the terminal error.

Use `Done()` when you need `select`-based coordination:

```go
handle, err := queue.Submit(ctx, job)
if err != nil {
	return err
}

select {
case <-handle.Done():
	result, ok := handle.Result()
	if ok && result.Err != nil {
		log.Printf("job failed: %v", result.Err)
	}
case <-time.After(2 * time.Second):
	log.Printf("job still running after 2s")
}
```

Use `WithNonBlocking()` when you want submit-time rejection instead of waiting
for queue capacity:

```go
handle, err := queue.Submit(ctx, job, cq.WithNonBlocking())
if err != nil {
	switch {
	case errors.Is(err, cq.ErrQueueFull):
		// Queue is currently full.
	case errors.Is(err, cq.ErrQueuePaused):
		// Rejected due to PauseReject behavior.
	case errors.Is(err, cq.ErrQueueStopped):
		// Queue is shutting down/stopped.
	}
	return err
}

_ = handle
```

## Cancelling Jobs

```go
if handle.Cancel() {
	log.Printf("cancellation accepted")
}
```

`Cancel` returns `true` when that call:

- Prevents a pending job from executing.
- Delivers the first cancellation request to a running job.

It returns `false` when cancellation was already requested or the handle was
already terminal.

A pending cancellation completes the handle with `cq.ErrJobCancelled`. A
running cancellation signals the job context. The job must respect context
cancellation to stop. If it ignores cancellation and returns normally, its
actual result is preserved.

```go
job := func(ctx context.Context) error {
	if err := fetchData(ctx); err != nil {
		return err
	}
	return saveData(ctx)
}
```

In this example, `fetchData` and `saveData` must also respect context
cancellation.

Cancelling a buffered job prevents its body from running but does not physically
remove its internal queue entry or immediately reclaim that buffer slot.
A worker or queue shutdown eventually consumes or discards the entry.

Cancelling the context passed to `handle.Wait(ctx)` stops only that wait. Use
`handle.Cancel()` to cancel the job.

Once the base queue processes a cancelled job, it increments
`TallyOf(cq.JobStateCancelled)` and `Stats().CancelledJobs`. A cancelled
buffered entry is counted when a worker consumes it. Jobs cancelled before
they reach the base queue are not included in its tallies.

Typed submission rejections include:

- `cq.ErrQueueStopped`
- `cq.ErrQueuePaused`
- `cq.ErrQueueFull`
- `cq.ErrQueueJobRequired` *(nil job submission)*

## Submission Options

`Submit`, `SubmitAfter`, and routed submission APIs accept `SubmitOption`s:

- `WithNonBlocking()`  
  Return `cq.ErrQueueFull` instead of waiting for capacity.
- `WithJobID(id)`  
  Set an explicit submission ID *(otherwise internal ID generation happens)*.
- `WithJobName(name)`  
  Set human-readable job metadata.
- `WithJobAttribute(key, value)`  
  Add one metadata attribute.
- `WithJobAttributes(map[string]string)`  
  Set metadata attributes as a map.

```go
handle, err := queue.Submit(
	ctx,
	job,
	cq.WithJobID("message-123"),
	cq.WithJobName("process-message"),
	cq.WithJobAttribute("source", "sqs"),
	cq.WithNonBlocking(),
)
```

## Other Submission APIs

All submission APIs return the same cancellable `JobHandle` values:

- `SubmitAfter` and `Reschedule` return handles that can cancel before delayed
  handoff.
- Batch methods return one independently cancellable handle per accepted job.
- `QueueManager` and `PriorityQueueManager` return handles from their routed
  queue.
- `PriorityQueue` handles can cancel while delayed, priority-buffered, or
  running.
- Scheduler `Latest` returns the latest occurrence's job handle. Cancelling the
  `ScheduleHandle` prevents future occurrences. Cancelling the latest
  `JobHandle` affects only that occurrence.

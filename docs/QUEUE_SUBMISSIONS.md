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
remove its internal queue entry or immediately reclaim that buffer slot. A worker
or queue shutdown eventually consumes or discards the entry.

Cancelling the context passed to `handle.Wait(ctx)` stops only that wait. Use
`handle.Cancel()` to cancel the job.

Once the base queue processes a cancelled job, it increments
`TallyOf(cq.JobStateCancelled)` and `Stats().CancelledJobs`. A cancelled buffered
entry is counted when a worker consumes it. Jobs cancelled before they reach the
base queue are not included in its tallies.

## Other Submission APIs

All submission APIs return the same cancellable `JobHandle` values:

- `SubmitAfter` and `Reschedule` return handles that can cancel before delayed handoff.
- Batch methods return one independently cancellable handle per accepted job.
- `QueueManager` and `PriorityQueueManager` return handles from their routed queue.
- `PriorityQueue` handles can cancel while delayed, priority-buffered, or running.
- Scheduler `Latest` returns the latest occurrence's job handle. Cancelling the
  `ScheduleHandle` prevents future occurrences. Cancelling the latest `JobHandle`
  affects only that occurrence.

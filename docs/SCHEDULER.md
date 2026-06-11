# Scheduler

Schedule recurring and one-time queue submissions. Each registered schedule
returns a handle for cancellation, lifecycle observation, and access to its
latest submission attempt.

```go
queue := cq.NewQueue(2, 10, 100)
queue.Start()

scheduler := cq.NewScheduler(context.Background(), queue)
defer scheduler.Stop()

cleanup, err := scheduler.Every(
	"cleanup",
	10*time.Minute,
	cleanupJob,
	cq.WithJobName("cleanup"),
	cq.WithJobAttribute("team", "operations"),
)

reminder, err := scheduler.At(
	"reminder",
	time.Now().Add(time.Hour),
	reminderJob,
)
```

`Every` and `At` accept normal `SubmitOption` values. The options are applied to
every submission triggered by that schedule.

## Schedule Handles

```go
handle, submitErr, attempted := cleanup.Latest()
if attempted && submitErr != nil {
	log.Printf("latest cleanup submission was rejected: %v", submitErr)
}
if handle != nil {
	// Cancel only the latest submitted occurrence.
	handle.Cancel()
	_ = handle.Wait(context.Background())
}

log.Printf("submission attempts: %d", cleanup.SubmissionAttempts())
cleanup.Cancel()
<-cleanup.Done()
```

`Latest` reports queue acceptance failures such as `cq.ErrQueueFull` or
`cq.ErrQueueStopped`. Cancelling a schedule prevents future submission attempts;
it does not cancel jobs already accepted by the queue. Use the `JobHandle`
returned by `Latest` to cancel one accepted occurrence.

The scheduler also supports lookup and centralized removal:

```go
scheduler.Has("cleanup")
scheduler.Remove("cleanup")
scheduler.List()
scheduler.Count()
```

Schedule registration errors:

- `cq.ErrScheduleExists`
- `cq.ErrScheduleIntervalInvalid`
- `cq.ErrScheduleInPast`
- `cq.ErrSchedulerStopped`

## Cron-Like Scheduling

Cron expressions can be supported externally by calculating each next run and
registering it with `At`. After the returned schedule handle reaches `Done`,
calculate and register the next occurrence. This keeps cron parsing and
persistence outside the in-memory scheduler.

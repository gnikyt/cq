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
`cq.ErrQueueStopped`. Cancelling a schedule prevents future submission attempts.
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
- `cq.ErrScheduleRequired`

## Cron Scheduling

`On` registers a recurring schedule driven by a `Schedule` implementation.
`ParseCron` provides a zero-dependency implementation for standard five-field
cron expressions:

```go
nightly, err := cq.ParseCron("0 2 * * *") // Every day at 02:00.
if err != nil {
	log.Fatal(err)
}

report, err := scheduler.On("nightly-report", nightly, reportJob)
```

Supported syntax: `*`, values, ranges (`a-b`), steps (`*/n`, `a-b/n`), lists
(`a,b-c`), month and day names (`jan`-`dec`, `sun`-`sat`), and the `@hourly`,
`@daily`, `@midnight`, `@weekly`, `@monthly`, `@yearly`/`@annually`
descriptors. Invalid expressions return
`cq.ErrCronExprInvalid` where `MustParseCron` panics instead.

### Custom Schedules

Any type implementing `Schedule` can drive `On`:

```go
type Schedule interface {
	// Next returns the next fire time strictly after `after`.
	// Returning the zero time (or a time not after `after`) ends the schedule.
	Next(after time.Time) time.Time
}
```

This is the extension point for third-party cron libraries, business-day
calendars, or jittered schedules. When `Next` returns the zero time the
schedule completes: it is removed from the scheduler and its handle's `Done`
channel closes.

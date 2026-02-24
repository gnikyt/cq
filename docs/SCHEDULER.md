# Scheduler

Schedule jobs to run at intervals or specific times. Useful for recurring tasks like cleanup, syncs, or reports, and one-time delayed operations.

```go
queue := cq.NewQueue(2, 10, 100)
queue.Start()

scheduler := cq.NewScheduler(context.Background(), queue)
defer scheduler.Stop()

scheduler.Every("cleanup", 10*time.Minute, cleanupJob) // Every 10 minutes.
scheduler.At("reminder", time.Now().Add(1*time.Hour), reminderJob) // 1 hour from now.

scheduler.Has("cleanup")
scheduler.Remove("cleanup")
scheduler.List()
scheduler.Count()
```

#### Cron-like Scheduling

**What it does:** Implements cron behavior by recursively scheduling next run times.

**When to use:** You need cron expressions without built-in cron parser support.

**Caveat:** Operationally, unhandled cron parse/reschedule errors can stop future runs silently.

```go
// ScheduleCron uses recursion to simulate cron behavior.
// After each execution, the job re-schedules itself for the next cron tick.
func ScheduleCron(s *cq.Scheduler, id, expr string, job cq.Job) error {
	gron := gronx.New()
	if !gron.IsValid(expr) {
		return fmt.Errorf("SchedulerCron: invalid cron: %s", expr)
	}

	// Calculate the first run time.
	nextRun, _ := gronx.NextTick(expr, true)

	// Create a self-scheduling job wrapper.
	var scheduled cq.Job
	scheduled = func(ctx context.Context) error {
		err := job(ctx) // Execute the actual job.
		// After completion, calculate and schedule the next run.
		if next, e := gronx.NextTick(expr, false); e == nil {
			s.At(id, next, scheduled) // Recursively re-schedule itself.
		}
		return err
	}

	// Schedule the first run.
	return s.At(id, nextRun, scheduled)
}

ScheduleCron(scheduler, "daily", "0 2 * * *", dailyJob)
// ScheduleCron(scheduler, "daily", "@daily", dailyJob)
```

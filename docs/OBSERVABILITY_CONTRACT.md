# Observability Contract

Defines lifecycle and telemetry for queue hooks and stats.

## Scope

Applies to:

- Queue lifecycle hooks configured with `WithHooks`.
- `JobEvent` payload fields passed to hooks.
- Queue counters exposed by `QueueStats` and `TallyOf`.

This does **not** define external delivery guarantees *(for example
exactly-once processing across process crashes)*.

## Hook Model

`Hooks` supports these callbacks:

- `OnEnqueue`
- `OnStart`
- `OnSuccess`
- `OnFailure`
- `OnDiscard`
- `OnAbandon`
- `OnReschedule`
- `OnAttemptStart`
- `OnAttemptSuccess`
- `OnAttemptFailure`

### Callback context

- `OnEnqueue` receives the submit/acceptance context.
- Execution callbacks (`OnStart`, `OnSuccess`, `OnFailure`, `OnDiscard`,
  attempt callbacks) receive the job execution context.
- `OnReschedule` receives the context used to request the reschedule.
- `OnAbandon` receives the queue context, which is usually already cancelled
  by the time shutdown dispatches it.
- Result callbacks may observe a cancelled context. Use
  `context.WithoutCancel(ctx)` if reporting must outlive cancellation.

## Lifecycle Semantics

For a single queue execution:

1. `OnEnqueue` fires after acceptance.
2. `OnStart` fires when a worker begins execution.
3. Exactly one terminal callback fires:
   - `OnSuccess` for successful completion.
   - `OnFailure` for non-discard errors *(including cancellation)*.
   - `OnDiscard` for discarded outcomes.
   - `OnAbandon` when shutdown ends the job before it starts. `OnStart` never
     fired for these, so they have no execution timings.

### Discard behavior

- Discarded outcomes emit `OnDiscard` only.
- Discarded outcomes do **not** emit `OnFailure`.

### Abandon behavior

Accepted jobs that never start still reach a terminal callback:

- `StopDrain` emits `OnAbandon` with `Err` matching `ErrQueueDrained` for every
  handed-back job *(buffered and delayed)*.
- `Stop(false)`, `Terminate`, and a `StopContext`/`StopTimeout` deadline emit
  `OnAbandon` with `Err` matching `ErrJobAbandoned` for pending submissions.
- `State` is `JobStateAbandoned`. There is no abandoned tally: drained jobs roll
  their enqueue accounting back as if never accepted.
- Abandon events are collected during shutdown and dispatched **after** the
  queue releases its acceptance lock, so a hook may call back into the queue
  without deadlocking. Consequently they arrive after the shutdown call has
  done its work, and their relative order across jobs is not defined.

### Retry attempt behavior

When retries are handled by `WithRetryPolicy`, each attempt emits:

- `OnAttemptStart` at attempt start.
- One of:
  - `OnAttemptSuccess` when that attempt succeeds.
  - `OnAttemptFailure` when that attempt returns an error.

Attempt callbacks are additive to queue-level callbacks. Queue-level terminal
callbacks still describe the overall execution outcome.

### Reschedule behavior

- `OnReschedule` fires when `Reschedule` successfully accepts the new
  submission.
- The event captures:
  - `Delay`
  - `RescheduleReason`
  - parent/root lineage via attributes when metadata is available.

Each hop is a **separate submission with its own `ID`**. Consumers that present
a single logical job across hops should group by the `cq.reschedule.root_id`
attribute, not by `ID`. The rescheduling execution still reports its own
terminal callback *(usually `OnSuccess`)* once it returns, so a terminal event
on one hop does not mean the logical job is finished. `WithRelease` and
`WithReleaseSelf` route through the same mechanism.

## `JobEvent` Field Semantics

Identity and correlation:

- `ID`: submission identifier.
- `Name`: optional job name.
- `QueueName`: optional queue name from `WithQueueName`.
- `Attributes`: cloned metadata attributes.

Timing:

- `EnqueuedAt`: acceptance timestamp of the current submission.
- `StartedAt`: worker execution start timestamp (when applicable).
- `FinishedAt`: execution finish timestamp (when applicable).
- `WaitDuration`: `StartedAt - EnqueuedAt` when both are present.
- `ExecutionDuration`: `FinishedAt - StartedAt` when both are present.

Execution:

- `Attempt`: attempt index from `JobMeta` (0-based).
- `State`: event state classification.
- `Err`: terminal or attempt error for failure-style events.

Reschedule:

- `Delay`: requested delay before resubmission.
- `RescheduleReason`: reason string (wrapper-defined or custom).

## Queue Counters Contract

`QueueStats` is a snapshot, not a transactionally consistent view across all
fields.

Core tallies:

- `CreatedJobs`
- `PendingJobs`
- `ActiveJobs`
- `FailedJobs`
- `DiscardedJobs`
- `CancelledJobs`
- `CompletedJobs`

Reschedule tallies:

- `RescheduledJobs`: total successful reschedule requests observed by queue
  hooks.
- `ReleasedJobs`: subset of reschedules with release semantics (`release` /
  `release_self` reasons).

State lookup:

- `TallyOf(JobState*)` supports created, pending, active, failed, discarded,
  cancelled, completed. Abandon has no tally.

## Ordering and Safety Notes

- Multiple `WithHooks(...)` registrations are appended and executed in
  registration order.
- **`OnStart` may fire before `OnEnqueue` for the same job.** A job is placed on
  the queue's buffer before its enqueue event is dispatched, so a worker can
  begin executing it first. The window is small, but a slow `OnEnqueue` widens
  it. Consumers that persist events must be order-tolerant: upsert by job ID
  with a state precedence rule rather than assuming `OnEnqueue` arrives first.
- Hooks are invoked **synchronously on the calling goroutine**: `OnEnqueue` on
  the submitting goroutine, execution callbacks on the worker goroutine. A hook
  that blocks holds up that goroutine, and slow execution hooks directly reduce
  throughput. Hooks that do I/O should hand off to their own buffered worker
  rather than writing inline.
- Hook panics are recovered and routed through queue panic handling.
- Hook payload maps are cloned before callback invocation to avoid shared
  mutable state.
- Hooks are observational. They do not alter queue execution decisions.

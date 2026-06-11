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
- `OnReschedule`
- `OnAttemptStart`
- `OnAttemptSuccess`
- `OnAttemptFailure`

### Callback context

- `OnEnqueue` receives the submit/acceptance context.
- Execution callbacks (`OnStart`, `OnSuccess`, `OnFailure`, `OnDiscard`,
  attempt callbacks) receive the job execution context.
- `OnReschedule` receives the context used to request the reschedule.
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

### Discard behavior

- Discarded outcomes emit `OnDiscard` only.
- Discarded outcomes do **not** emit `OnFailure`.

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
  cancelled, completed.

## Ordering and Safety Notes

- Multiple `WithHooks(...)` registrations are appended and executed in
  registration order.
- Hook panics are recovered and routed through queue panic handling.
- Hook payload maps are cloned before callback invocation to avoid shared
  mutable state.
- Hooks are observational. They do not alter queue execution decisions.

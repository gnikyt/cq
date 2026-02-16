# Envelope Persistence and Recovery

Use an `EnvelopeStore` to observe queue lifecycle transitions and optionally persist/replay recoverable jobs after restart/outage. It does not have to be durable storage only... you can also use it for event-driven side effects such as DLQ routing, failure notifications, or metrics hooks. Persistence remains useful when you need durability and observability beyond in-memory queue state (process crashes, deploy restarts, audit trails, delayed/released job tracking). Implementations can use Redis, SQL/NoSQL databases, files, object storage, or non-persistent adapters that forward events.

```go
queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(store))
```

## Lifecycle Callbacks

`EnvelopeStore` receives lifecycle callbacks:

- `Enqueue` when accepted by queue.
- `Claim` when a worker starts execution.
- `Ack` on success.
- `Nack` on failure.
- `Reschedule` when execution is intentionally deferred.
- `Recoverable` when loading jobs to replay.

Built-in deferred wrappers automatically call `Reschedule`:

- `WithRelease` -> `cq.EnvelopeRescheduleReasonRelease`
- `WithReleaseSelf` -> `cq.EnvelopeRescheduleReasonReleaseSelf`
- `WithRateLimitRelease` -> `cq.EnvelopeRescheduleReasonRateLimit`

## Recovery

Use `RecoverEnvelopes` to rebuild jobs and enqueue them from persisted envelopes:

```go
registry := cq.NewEnvelopeRegistry()
registry.Register("send_invitation", func(env cq.Envelope) (cq.Job, error) {
	var payload SendInvitationPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return nil, err
	}

	baseJob := func(ctx context.Context) error {
		return sendEmail(ctx, payload)
	}
	return cq.WithRetry(
		cq.WithBackoff(baseJob, cq.ExponentialBackoff),
		3,
	), nil
})

scheduled, err := cq.RecoverEnvelopes(context.Background(), queue, store, registry, time.Now())
if err != nil {
	log.Fatal(err)
}
log.Printf("scheduled %d recovered jobs", scheduled)
```

For larger systems, use `RecoverEnvelopesWithOptions` for batched/lenient recovery and `StartRecoveryLoop` to poll recoverable jobs continuously:

```go
cancelLoop, err := cq.StartRecoveryLoop(
	context.Background(),
	5*time.Second, // Poll interval.
	queue,
	store,
	registry,
	cq.RecoverOptions{
		MaxEnvelopes:    100,
		ContinueOnError: true,
		OnError: func(env cq.Envelope, err error) {
			log.Printf("recover skipped (id=%s type=%s): %v", env.ID, env.Type, err)
		},
	},
	func(err error) {
		log.Printf("recover loop error: %v", err)
	},
)
if err != nil {
	log.Fatal(err)
}
defer cancelLoop()
```

Recovery timing behavior:

- If `Envelope.NextRunAt` is zero or in the past, recovery enqueues immediately.
- If `Envelope.NextRunAt` is in the future, recovery uses `DelayEnqueue` for that remaining duration.

## Store Implementations

**What it does:** Shows concrete `EnvelopeStore` implementations for different persistence goals.
**When to use:** You need recovery, auditability, DLQ routing, or custom side effects.
**Example:** See the DLQ, file-backed, and DynamoDB snippets below.
**Caveat:** Operationally, use durable storage when restart recovery guarantees are required.

### DLQ-only Example

```go
type deadLetterStore struct {
	db *sql.DB
}

func (s *deadLetterStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Claim(ctx context.Context, id string) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Ack(ctx context.Context, id string) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	return nil, nil // No replay in this implementation.
}

func (s *deadLetterStore) Nack(ctx context.Context, id string, reason error) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO dead_letter_jobs (job_id, failed_at, error_text) VALUES (?, ?, ?)`,
		id,
		time.Now().UTC(),
		reason.Error(),
	)
	return err
}

queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(&deadLetterStore{db: db}))
```

### File-backed Example

Minimal file-backed `EnvelopeStore` example (JSON file):

```go
type fileEnvelopeStore struct {
	mu   sync.Mutex
	path string
}

func (s *fileEnvelopeStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	return s.update(env.ID, func(e *cq.Envelope) {
		*e = env
		e.Status = cq.EnvelopeStatusEnqueued
	})
}

func (s *fileEnvelopeStore) Claim(ctx context.Context, id string) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Status = cq.EnvelopeStatusClaimed
		e.ClaimedAt = time.Now()
	})
}

func (s *fileEnvelopeStore) Ack(ctx context.Context, id string) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Status = cq.EnvelopeStatusAcked
	})
}

func (s *fileEnvelopeStore) Nack(ctx context.Context, id string, reason error) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Status = cq.EnvelopeStatusNacked
		e.LastError = reason.Error()
	})
}

func (s *fileEnvelopeStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Status = cq.EnvelopeStatusEnqueued
		e.NextRunAt = nextRunAt
		e.LastError = reason
	})
}

func (s *fileEnvelopeStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := s.readAll()
	if err != nil {
		return nil, err
	}

	out := make([]cq.Envelope, 0, len(records))
	for _, env := range records {
		if env.Status == cq.EnvelopeStatusAcked {
			continue
		}
		if !env.NextRunAt.IsZero() && env.NextRunAt.After(now) {
			continue
		}
		out = append(out, env)
	}
	return out, nil
}

// Helpers to implement:
// - readAll(): loads map[string]cq.Envelope from s.path (json.Unmarshal).
// - writeAll(): persists map[string]cq.Envelope to s.path (json.MarshalIndent).
// - update(id, fn): loads map, applies fn on map[id], then writes map back.
```

### DynamoDB Example

```go
type dynamoEnvelopeStore struct {
	client *dynamodb.Client
	table  string
}

func (s *dynamoEnvelopeStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	env.Status = cq.EnvelopeStatusEnqueued
	return s.putEnvelope(ctx, env)
}

func (s *dynamoEnvelopeStore) Claim(ctx context.Context, id string) error {
	return s.updateStatus(ctx, id, cq.EnvelopeStatusClaimed, time.Now(), nil, "")
}

func (s *dynamoEnvelopeStore) Ack(ctx context.Context, id string) error {
	return s.updateStatus(ctx, id, cq.EnvelopeStatusAcked, time.Time{}, nil, "")
}

func (s *dynamoEnvelopeStore) Nack(ctx context.Context, id string, reason error) error {
	return s.updateStatus(ctx, id, cq.EnvelopeStatusNacked, time.Time{}, nil, reason.Error())
}

func (s *dynamoEnvelopeStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return s.updateStatus(ctx, id, cq.EnvelopeStatusEnqueued, time.Time{}, &nextRunAt, reason)
}

func (s *dynamoEnvelopeStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	// Query by status/time using a GSI such as:
	// GSI1PK = status, GSI1SK = next_run_at (unix timestamp).
	//
	// Recoverable rule:
	// - exclude acked
	// - include next_run_at <= now (or missing/zero)
	return s.queryRecoverable(ctx, now)
}

func (s *dynamoEnvelopeStore) putEnvelope(ctx context.Context, env cq.Envelope) error {
	item, err := attributevalue.MarshalMap(env)
	if err != nil {
		return err
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	})
	return err
}

func (s *dynamoEnvelopeStore) updateStatus(
	ctx context.Context,
	id string,
	status string,
	claimedAt time.Time,
	nextRunAt *time.Time,
	lastError string,
) error {
	// Use UpdateItem to set status and optional fields atomically.
	// Example updates:
	// - status
	// - claimed_at (on claim)
	// - next_run_at + last_error (on reschedule/nack)
	// Keep this single-write to avoid partial state transitions.
	return nil
}

func (s *dynamoEnvelopeStore) queryRecoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	// Use Query (preferred with GSI) or Scan (small datasets).
	// Unmarshal each item back into cq.Envelope.
	return nil, nil
}

store := &dynamoEnvelopeStore{
	client: dynamoClient,
	table:  "cq_envelopes",
}
queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(store))
```

Suggested table configuration:

- Partition key: `id` (string)
- Attributes: `type`, `status`, `payload`, `enqueued_at`, `claimed_at`, `next_run_at`, `last_error`
- Optional GSI for recovery polling: (`status`, `next_run_at`)

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

## Handler-First Payload Metadata (Recommended)

Use a first-class typed envelope handler so type/payload are persisted at enqueue time:

```go
sendInvitation := cq.EnvelopeHandler[SendInvitationPayload]{
	Type:  "send_invitation", // Job type identifier.
	Codec: cq.EnvelopeJSONCodec[SendInvitationPayload](), // Codec used to encode and decode the payload.
	Handler: func(ctx context.Context, payload SendInvitationPayload) error {
		log.Printf("sending invitation to=%s", payload.Email)
		return nil
	}, // Handler which is job-compatible and accepts the payload.
}

err := cq.EnqueueEnvelope(queue, sendInvitation, SendInvitationPayload{Email: "demo@example.com"})
if err != nil {
	log.Fatal(err)
}

// or...

// Batch enqueue for the same handler.
err = cq.EnqueueEnvelopeBatch(queue, sendInvitation, []SendInvitationPayload{
	{Email: "a@example.com"},
	{Email: "b@example.com"},
})
if err != nil {
	log.Fatal(err)
}

// or...

// Non-blocking batch enqueue returns the number accepted.
accepted, err := cq.TryEnqueueEnvelopeBatch(queue, sendInvitation, []SendInvitationPayload{
	{Email: "c@example.com"},
	{Email: "d@example.com"},
})
if err != nil {
	log.Fatal(err)
}
log.Printf("accepted %d", accepted)
```

Build your own codec when JSON is not the desired format:

```go
codec := cq.EnvelopeCodec[SendInvitationPayload]{
	Marshal: func(v SendInvitationPayload) ([]byte, error) {
		return XMLMarshal(v)
	},
	Unmarshal: func(payload []byte, out *SendInvitationPayload) error {
		return XMLUnmarshal(payload, out)
	},
}
```

## Advanced Runtime Overrides

The handler-first path is the default. If needed, you can still override envelope payload during a job runtime:

```go
codec := cq.EnvelopeJSONCodec[OrderProcessPayload]()

processOrder := cq.EnvelopeHandler[OrderProcessPayload]{
	Type:  "process_order",
	Codec: codec,
	Handler: func(ctx context.Context, payload OrderProcessPayload) error {
		// Step 1: reserve inventory (already completed in this example).
		if payload.Step == "reserve_inventory" {
			payload.Step = "charge_payment"

			// Persist progress so retry/recovery restarts at charge step.
			updated, err := codec.Marshal(payload)
			if err != nil {
				return err
			}
			cq.SetEnvelopePayload(ctx, "process_order", updated)
		}
		return nil
	},
}

err := cq.EnqueueEnvelope(queue, processOrder, OrderProcessPayload{
	OrderID:  "ord_123",
	Step:     "reserve_inventory",
})
if err != nil {
	log.Fatal(err)
}
```

`SetEnvelopePayload` is last-write-wins per run.

## Recovery

Use `RegisterEnvelopeHandler` + `RecoverEnvelopes` to rebuild jobs and enqueue them from persisted envelopes:

```go
registry := cq.NewEnvelopeRegistry()
sendInvitation := cq.EnvelopeHandler[SendInvitationPayload]{
	Type:  "send_invitation",
	Codec: cq.EnvelopeJSONCodec[SendInvitationPayload](),
	Handler: func(ctx context.Context, payload SendInvitationPayload) error {
		return cq.WithRetry(
			cq.WithBackoff(
				func(ctx context.Context) error {
					log.Printf("sending invitation (to=%s)", payload.Email)
					return nil
				},
				cq.ExponentialBackoff,
			),
			3,
		)(ctx)
	},
}

cq.RegisterEnvelopeHandler(registry, sendInvitation)

scheduled, err := cq.RecoverEnvelopes(context.Background(), queue, store, registry, time.Now())
if err != nil {
	log.Fatal(err)
}
log.Printf("scheduled %d recovered jobs", scheduled)
```

Manual registry wiring is still available when you need full control:

```go
registry.Register("send_invitation", func(env cq.Envelope) (cq.Job, error) {
	var payload SendInvitationPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		log.Printf("sending invitation (to=%s)", payload.Email)
		return nil
	}, nil
})
```

Recover a single envelope by ID (for example from an admin HTTP replay endpoint):

```go
scheduled, err := cq.RecoverEnvelopeByID(
	context.Background(),
	queue,
	store,
	registry,
	"job-id-123",
	time.Now(),
)
if err != nil {
	log.Fatal(err)
}
log.Printf("scheduled single envelope: %v", scheduled)
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

## Failed / Dead Set Operations

For stores that expose nacked-envelope listing, use:

```go
page, err := cq.ListNackedEnvelopes(
	context.Background(),
	store,
	cq.NackedEnvelopeQuery{Limit: 50, Cursor: "", Type: "send_invitation"},
)
if err != nil {
	log.Fatal(err)
}
log.Printf("failed=%d next=%q", len(page.Envelopes), page.NextCursor)
```

Retry a failed envelope by ID:

```go
ok, err := cq.RetryNackedEnvelopeByID(
	context.Background(),
	queue,
	store,
	registry,
	"job-id-123",
	time.Now(),
)
if err != nil {
	log.Fatal(err)
}
log.Printf("retried=%v", ok)
```

`RetryNackedEnvelopeByID` timing behavior:
- If `retryAt` is zero, in the past, or now: retry is enqueued immediately.
- If `retryAt` is in the future: retry is delayed until `retryAt`.
- For immediate retries, stores can optionally implement `EnvelopeRetryStateStore`
  and persist an explicit retry transition via `MarkRetried(...)`.
- For delayed retries, `Reschedule(...)` is used to persist timing intent.

Discard a failed envelope by ID:

```go
if err := cq.DiscardNackedEnvelopeByID(context.Background(), store, "job-id-123"); err != nil {
	log.Fatal(err)
}
```

Notes:
- `ListNackedEnvelopes` is cursor-paginated and returns `NackedEnvelopePage` (`Envelopes`, `NextCursor`).
- `ListNackedEnvelopes` requires store support for `NackedEnvelopeLister`... otherwise it returns `cq.ErrNackedListUnsupported`.
- `RetryNackedEnvelopeByID` and `DiscardNackedEnvelopeByID` require target envelope status to be `nacked`... otherwise they return `cq.ErrEnvelopeNotNacked`.

Optional retry transition interface:

```go
type EnvelopeRetryStateStore interface {
	MarkRetried(ctx context.Context, id string, retriedAt time.Time, reason string) error
}
```

## Store Implementations

**What it does:** Shows concrete `EnvelopeStore` implementations for different persistence goals.

**When to use:** You need recovery, auditability, DLQ routing, or custom side effects.

**Example:** See the DLQ, file-backed, and DynamoDB snippets below.

**Caveat:** Operationally, use durable storage when restart recovery guarantees are required.

### DLQ-only Example

```go
type emailPayload struct {
	Email string `json:"email"`
}

type deadLetterStore struct {
	db *sql.DB

	mu sync.Mutex
	envelopes map[string]cq.Envelope // Keeps latest type/payload by job ID so Nack records useful context.
}

func (s *deadLetterStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.envelopes == nil {
		s.envelopes = make(map[string]cq.Envelope)
	}
	s.envelopes[env.ID] = cq.Envelope{
		ID:      env.ID,
		Type:    env.Type,
		Payload: env.Payload,
	}

	return nil 
}

func (s *deadLetterStore) Claim(ctx context.Context, id string) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Ack(ctx context.Context, id string) error {
	s.mu.Lock()
	delete(s.envelopes, id) // Terminal state in this process to avoid map growth.
	s.mu.Unlock()

	return nil
}

func (s *deadLetterStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	return nil // Not needed for DLQ-only usage.
}

func (s *deadLetterStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	return nil, nil // No replay in this implementation.
}

func (s *deadLetterStore) RecoverByID(ctx context.Context, id string) (cq.Envelope, error) {
	return cq.Envelope{}, cq.ErrEnvelopeNotFound // No replay in this implementation.
}

func (s *deadLetterStore) Nack(ctx context.Context, id string, reason error) error {
	s.mu.Lock()
	env := s.envelopes[id]
	s.mu.Unlock()

	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO dead_letter_jobs (job_id, job_type, payload_json, failed_at, error_text) VALUES (?, ?, ?, ?, ?)`,
		id,
		env.Type,
		string(env.Payload),
		time.Now().UTC(),
		reason.Error(),
	)
	if err == nil {
		s.mu.Lock()
		delete(s.envelopes, id) // Terminal state after DLQ write.
		s.mu.Unlock()
	}
	return err
}

func (s *deadLetterStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.envelopes == nil {
		s.envelopes = make(map[string]cq.Envelope)
	}

	env := s.envelopes[id]
	env.ID = id
	env.Type = typ
	env.Payload = payload
	s.envelopes[id] = env

	return nil
}

store := &deadLetterStore{db: db}
queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(store))

sendEmail := cq.EnvelopeHandler[emailPayload]{
	Type:  "send_email",
	Codec: cq.EnvelopeJSONCodec[emailPayload](),
	Handler: func(ctx context.Context, payload emailPayload) error {
		return cq.WithRetry(
			cq.WithBackoff(
				func(ctx context.Context) error {
					return errors.New("smtp down")
				},
				cq.ExponentialBackoff,
			),
			3,
		)(ctx) // Retries first, then triggers Nack -> DLQ insert with payload context.
	},
}

if err := cq.EnqueueEnvelope(queue, sendEmail, emailPayload{Email: "demo@example.com"}); err != nil {
	log.Fatal(err)
}
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
		e.Payload = env.Payload
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

func (s *fileEnvelopeStore) RecoverByID(ctx context.Context, id string) (cq.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := s.readAll()
	if err != nil {
		return cq.Envelope{}, err
	}
	env, ok := records[id]
	if !ok {
		return cq.Envelope{}, cq.ErrEnvelopeNotFound
	}
	return env, nil
}

func (s *fileEnvelopeStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	return s.update(id, func(e *cq.Envelope) {
		e.Type = typ
		e.Payload = payload
	})
}

// Helpers to implement:
// - readAll(): loads map[string]cq.Envelope from s.path (json.Unmarshal).
// - writeAll(): persists map[string]cq.Envelope to s.path (json.MarshalIndent).
// - update(id, fn): loads map, applies fn on map[id], then writes map back.

type invoicePayload struct {
	InvoiceID string `json:"invoice_id"`
	Region    string `json:"region"`
}

store := &fileEnvelopeStore{path: "/var/lib/cq/envelopes.json"}
queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(store))

processInvoice := cq.EnvelopeHandler[invoicePayload]{
	Type:  "process_invoice",
	Codec: cq.EnvelopeJSONCodec[invoicePayload](),
	Handler: func(ctx context.Context, payload invoicePayload) error {
		log.Printf("processing invoice %s (region=%s)", payload.InvoiceID, payload.Region)
		return nil
	},
}

if err := cq.EnqueueEnvelope(queue, processInvoice, invoicePayload{
	InvoiceID: "inv_123",
	Region:    "us-east-1",
}); err != nil {
	log.Fatal(err)
}
```

### DynamoDB Example

```go
type dynamoEnvelopeStore struct {
	client *dynamodb.Client
	table  string
}

func (s *dynamoEnvelopeStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	env.Status = cq.EnvelopeStatusEnqueued
	env.Payload = env.Payload
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

func (s *dynamoEnvelopeStore) RecoverByID(ctx context.Context, id string) (cq.Envelope, error) {
	// Use GetItem by partition key (id) and unmarshal into cq.Envelope.
	return cq.Envelope{}, nil
}

func (s *dynamoEnvelopeStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	// Use UpdateItem to atomically set replay metadata.
	// Example updates:
	// - type
	// - payload
	return nil
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
	// Replay metadata (type/payload) can be updated via SetPayload.
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

type shipmentPayload struct {
	ShipmentID string `json:"shipment_id"`
	Carrier    string `json:"carrier"`
}

shipOrder := cq.EnvelopeHandler[shipmentPayload]{
	Type:  "ship_order",
	Codec: cq.EnvelopeJSONCodec[shipmentPayload](),
	Handler: func(ctx context.Context, payload shipmentPayload) error {
		log.Printf("shipping %s (carrier=%s)", payload.ShipmentID, payload.Carrier)
		return nil
	},
}

if err := cq.EnqueueEnvelope(queue, shipOrder, shipmentPayload{
	ShipmentID: "shp_123",
	Carrier:    "ups",
}); err != nil {
	log.Fatal(err)
}
```

Suggested table configuration:

- Partition key: `id` (string)
- Attributes: `type`, `status`, `payload`, `enqueued_at`, `claimed_at`, `next_run_at`, `last_error`
- Optional GSI for recovery polling: (`status`, `next_run_at`)

## End-to-End SQLite Example

The following example shows a SQLite-backed envelope store that supports:

- `EnvelopeStore` (persistence + recovery)
- `NackedEnvelopeLister` (cursor paging for nacked items)
- `EnvelopeRetryStateStore` (explicit immediate retry transition)

```go
package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/gnikyt/cq"
)

const (
	defaultNackedPageLimit = 50
	timeLayout             = time.RFC3339Nano
)

const (
	sqlMigrate = `
CREATE TABLE IF NOT EXISTS envelopes (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL DEFAULT '',
  payload BLOB NOT NULL DEFAULT X'',
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  enqueued_at TEXT,
  claimed_at TEXT,
  next_run_at TEXT,
  last_error TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_env_status_updated_id ON envelopes(status, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_env_status_next_run ON envelopes(status, next_run_at);
`

	sqlUpsertEnqueue = `
INSERT INTO envelopes (id, type, payload, status, attempt, enqueued_at, next_run_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  type=excluded.type,
  payload=excluded.payload,
  status=excluded.status,
  attempt=excluded.attempt,
  enqueued_at=excluded.enqueued_at,
  next_run_at=excluded.next_run_at,
  updated_at=excluded.updated_at
`

	sqlClaim = `
UPDATE envelopes
SET status=?, claimed_at=?, updated_at=?
WHERE id=?
`

	sqlAck = `
UPDATE envelopes
SET status=?, updated_at=?
WHERE id=?
`

	sqlNack = `
UPDATE envelopes
SET status=?, last_error=?, updated_at=?
WHERE id=?
`

	sqlReschedule = `
UPDATE envelopes
SET status=?, next_run_at=?, last_error=?, updated_at=?
WHERE id=?
`

	sqlSetPayload = `
UPDATE envelopes
SET type=?, payload=?, updated_at=?
WHERE id=?
`

	sqlRecoverable = `
SELECT id, type, payload, status, attempt, enqueued_at, claimed_at, next_run_at, last_error
FROM envelopes
WHERE status != ?
  AND (next_run_at IS NULL OR next_run_at = '' OR next_run_at <= ?)
ORDER BY updated_at ASC
`

	sqlRecoverByID = `
SELECT id, type, payload, status, attempt, enqueued_at, claimed_at, next_run_at, last_error
FROM envelopes
WHERE id=?
`
)

type sqliteEnvelopeStore struct {
	db *sql.DB
}

type nackedCursor struct {
	UpdatedAt string `json:"u"`
	ID        string `json:"i"`
}

func newSQLiteEnvelopeStore(db *sql.DB) *sqliteEnvelopeStore {
	return &sqliteEnvelopeStore{db: db}
}

func (s *sqliteEnvelopeStore) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, sqlMigrate)
	return err
}

func (s *sqliteEnvelopeStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	_, err := s.db.ExecContext(ctx, sqlUpsertEnqueue,
		env.ID,
		env.Type,
		env.Payload,
		cq.EnvelopeStatusEnqueued,
		env.Attempt,
		formatTime(env.EnqueuedAt),
		nullableTime(env.NextRunAt),
		nowString(),
	)
	return err
}

func (s *sqliteEnvelopeStore) Claim(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, sqlClaim, cq.EnvelopeStatusClaimed, nowString(), nowString(), id)
	return err
}

func (s *sqliteEnvelopeStore) Ack(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, sqlAck, cq.EnvelopeStatusAcked, nowString(), id)
	return err
}

func (s *sqliteEnvelopeStore) Nack(ctx context.Context, id string, reason error) error {
	msg := ""
	if reason != nil {
		msg = reason.Error()
	}
	_, err := s.db.ExecContext(ctx, sqlNack, cq.EnvelopeStatusNacked, msg, nowString(), id)
	return err
}

func (s *sqliteEnvelopeStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	_, err := s.db.ExecContext(ctx, sqlReschedule,
		cq.EnvelopeStatusEnqueued,
		formatTime(nextRunAt),
		reason,
		nowString(),
		id,
	)
	return err
}

func (s *sqliteEnvelopeStore) MarkRetried(ctx context.Context, id string, retriedAt time.Time, reason string) error {
	_, err := s.db.ExecContext(ctx, sqlReschedule,
		cq.EnvelopeStatusEnqueued,
		formatTime(retriedAt),
		reason,
		nowString(),
		id,
	)
	return err
}

func (s *sqliteEnvelopeStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	_, err := s.db.ExecContext(ctx, sqlSetPayload, typ, payload, nowString(), id)
	return err
}

func (s *sqliteEnvelopeStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	rows, err := s.db.QueryContext(ctx, sqlRecoverable, cq.EnvelopeStatusAcked, formatTime(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]cq.Envelope, 0, 64)
	for rows.Next() {
		env, err := scanEnvelope(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, rows.Err()
}

func (s *sqliteEnvelopeStore) RecoverByID(ctx context.Context, id string) (cq.Envelope, error) {
	row := s.db.QueryRowContext(ctx, sqlRecoverByID, id)
	env, err := scanEnvelope(row)
	if errors.Is(err, sql.ErrNoRows) {
		return cq.Envelope{}, cq.ErrEnvelopeNotFound
	}
	return env, err
}

// NackedEnvelopeLister: cursor pagination by (updated_at DESC, id DESC).
func (s *sqliteEnvelopeStore) Nacked(ctx context.Context, q cq.NackedEnvelopeQuery) (cq.NackedEnvelopePage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultNackedPageLimit
	}

	var cursor nackedCursor
	if q.Cursor != "" {
		raw, err := base64.StdEncoding.DecodeString(q.Cursor)
		if err != nil {
			return cq.NackedEnvelopePage{}, fmt.Errorf("invalid cursor: %w", err)
		}
		if err := json.Unmarshal(raw, &cursor); err != nil {
			return cq.NackedEnvelopePage{}, fmt.Errorf("invalid cursor payload: %w", err)
		}
	}

	args := []any{cq.EnvelopeStatusNacked}
	query := `
SELECT id, type, payload, status, attempt, enqueued_at, claimed_at, next_run_at, last_error, updated_at
FROM envelopes
WHERE status=?
`
	if q.Type != "" {
		query += " AND type=?"
		args = append(args, q.Type)
	}
	if cursor.UpdatedAt != "" && cursor.ID != "" {
		query += " AND (updated_at < ? OR (updated_at = ? AND id < ?))"
		args = append(args, cursor.UpdatedAt, cursor.UpdatedAt, cursor.ID)
	}
	query += " ORDER BY updated_at DESC, id DESC LIMIT ?"
	args = append(args, limit+1) // fetch one extra for next cursor.

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return cq.NackedEnvelopePage{}, err
	}
	defer rows.Close()

	type rowData struct {
		env       cq.Envelope
		updatedAt string
	}
	tmp := make([]rowData, 0, limit+1)
	for rows.Next() {
		var (
			id, typ, status, updatedAt string
			payload                    []byte
			attempt                    int
			enqueuedAt, claimedAt      sql.NullString
			nextRunAt, lastError       sql.NullString
		)
		if err := rows.Scan(
			&id, &typ, &payload, &status, &attempt,
			&enqueuedAt, &claimedAt, &nextRunAt, &lastError, &updatedAt,
		); err != nil {
			return cq.NackedEnvelopePage{}, err
		}
		tmp = append(tmp, rowData{
			env: cq.Envelope{
				ID:         id,
				Type:       typ,
				Payload:    payload,
				Status:     cq.EnvelopeStatus(status),
				Attempt:    attempt,
				EnqueuedAt: parseNullableTime(enqueuedAt),
				ClaimedAt:  parseNullableTime(claimedAt),
				NextRunAt:  parseNullableTime(nextRunAt),
				LastError:  lastError.String,
			},
			updatedAt: updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return cq.NackedEnvelopePage{}, err
	}

	page := cq.NackedEnvelopePage{
		Envelopes: make([]cq.Envelope, 0, limit),
	}
	for i, item := range tmp {
		if i == limit {
			cur, _ := json.Marshal(nackedCursor{UpdatedAt: item.updatedAt, ID: item.env.ID})
			page.NextCursor = base64.StdEncoding.EncodeToString(cur)
			break
		}
		page.Envelopes = append(page.Envelopes, item.env)
	}
	return page, nil
}

func scanEnvelope(scanner interface{ Scan(dest ...any) error }) (cq.Envelope, error) {
	var (
		id					  string
		typ					  string
		status                string
		payload               []byte
		attempt               int
		enqueuedAt, claimedAt sql.NullString
		nextRunAt, lastError  sql.NullString
	)
	if err := scanner.Scan(
		&id, &typ, &payload, &status, &attempt,
		&enqueuedAt, &claimedAt, &nextRunAt, &lastError,
	); err != nil {
		return cq.Envelope{}, err
	}
	return cq.Envelope{
		ID:         id,
		Type:       typ,
		Payload:    payload,
		Status:     cq.EnvelopeStatus(status),
		Attempt:    attempt,
		EnqueuedAt: parseNullableTime(enqueuedAt),
		ClaimedAt:  parseNullableTime(claimedAt),
		NextRunAt:  parseNullableTime(nextRunAt),
		LastError:  lastError.String,
	}, nil
}

func nowString() string {
	return time.Now().UTC().Format(timeLayout)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(timeLayout)
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(timeLayout)
}

func parseNullableTime(v sql.NullString) time.Time {
	if !v.Valid || v.String == "" {
		return time.Time{}
	}
	t, _ := time.Parse(timeLayout, v.String)
	return t
}
```

## Recovery Usage (Automatic)

Use this when you want startup or loop-driven replay from `Recoverable(...)`:

```go
db, _ := sql.Open("sqlite", "file:queue.db?_foreign_keys=on")
store := newSQLiteEnvelopeStore(db)
_ = store.Migrate(context.Background())

queue := cq.NewQueue(1, 10, 100, cq.WithEnvelopeStore(store))
queue.Start()
defer queue.Stop(true)

registry := cq.NewEnvelopeRegistry()
cq.RegisterEnvelopeHandler(registry, sendInvitationHandler)
cq.RegisterEnvelopeHandler(registry, processOrderHandler)

scheduled, err := cq.RecoverEnvelopes(context.Background(), queue, store, registry, time.Now())
if err != nil {
	log.Fatal(err)
}
log.Printf("recovered=%d", scheduled)

cancelLoop, err := cq.StartRecoveryLoop(
	context.Background(),
	5*time.Second,
	queue,
	store,
	registry,
	cq.RecoverOptions{MaxEnvelopes: 100, ContinueOnError: true},
	func(err error) {
		log.Printf("recover loop error: %v", err)
	},
)
if err != nil {
	log.Fatal(err)
}
defer cancelLoop()
```

## Recovery Usage (Manual Nacked Operations)

Use this when operators decide exactly what to retry/discard:

```go
cursor := ""
for {
	page, err := cq.ListNackedEnvelopes(context.Background(), store, cq.NackedEnvelopeQuery{
		Limit:  50,
		Type:   "send_invitation",
		Cursor: cursor,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, env := range page.Envelopes {
		// Example policy: delay-retry some, discard others.
		if shouldDiscard(env) {
			_ = cq.DiscardNackedEnvelopeByID(context.Background(), store, env.ID)
			continue
		}

		retryAt := time.Now().Add(10 * time.Second) // or time.Now() for immediate retry
		_, _ = cq.RetryNackedEnvelopeByID(context.Background(), queue, store, registry, env.ID, retryAt)
	}

	if page.NextCursor == "" {
		break
	}
	cursor = page.NextCursor
}
```

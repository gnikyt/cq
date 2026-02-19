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

## Store Implementations

**What it does:** Shows concrete `EnvelopeStore` implementations for different persistence goals.
**When to use:** You need recovery, auditability, DLQ routing, or custom side effects.
**Example:** See the SQS poller bridge, DLQ, file-backed, and DynamoDB snippets below.
**Caveat:** Operationally, use durable storage when restart recovery guarantees are required.

### SQS-backed Example

If you want SQS to be the store itself, map envelope lifecycle callbacks directly to SQS operations:

- `Enqueue` -> `SendMessage` (store envelope snapshot in message body)
- `Ack` -> `DeleteMessage` (remove from SQS when work succeeds)
- `Nack` -> no-op (or rely on visibility timeout / queue redrive policy)
- `Reschedule` -> `ChangeMessageVisibility` or re-send with delay

```go
type sqsEnvelopeStore struct {
	client *sqs.Client
	url    string

	// Runtime receipt handles by envelope ID.
	// Filled by your poller (ReceiveMessage) before cq execution starts.
	receipts sync.Map // map[string]string
}

func (s *sqsEnvelopeStore) Enqueue(ctx context.Context, env cq.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}

	_, err = s.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &s.url,
		MessageBody: aws.String(string(body)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"envelope_id": {DataType: aws.String("String"), StringValue: aws.String(env.ID)},
			"type":        {DataType: aws.String("String"), StringValue: aws.String(env.Type)},
		},
	})
	return err
}

func (s *sqsEnvelopeStore) Claim(ctx context.Context, id string) error { return nil }

func (s *sqsEnvelopeStore) RememberReceipt(id string, receiptHandle string) {
	s.receipts.Store(id, receiptHandle)
}

func (s *sqsEnvelopeStore) Ack(ctx context.Context, id string) error {
	raw, ok := s.receipts.Load(id)
	if !ok {
		return nil // Already deleted or not tracked in this process.
	}

	receipt := raw.(string)
	_, err := s.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &s.url,
		ReceiptHandle: &receipt,
	})
	if err == nil {
		s.receipts.Delete(id)
	}
	return err
}

func (s *sqsEnvelopeStore) Nack(ctx context.Context, id string, reason error) error {
	return nil // Let visibility timeout expire for retry, or rely on SQS redrive policy.
}

func (s *sqsEnvelopeStore) Reschedule(ctx context.Context, id string, nextRunAt time.Time, reason string) error {
	raw, ok := s.receipts.Load(id)
	if !ok {
		return nil
	}

	receipt := raw.(string)
	delay := int32(time.Until(nextRunAt).Seconds())
	if delay < 0 {
		delay = 0
	}
	_, err := s.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &s.url,
		ReceiptHandle:     &receipt,
		VisibilityTimeout: delay,
	})
	return err
}

func (s *sqsEnvelopeStore) SetPayload(ctx context.Context, id string, typ string, payload []byte) error {
	return nil // SQS messages are immutable... use Reschedule and new SendMessage pattern if needed.
}

func (s *sqsEnvelopeStore) Recoverable(ctx context.Context, now time.Time) ([]cq.Envelope, error) {
	return nil, nil // Recovery is delegated to SQS receive/redrive behavior.
}

func (s *sqsEnvelopeStore) RecoverByID(ctx context.Context, id string) (cq.Envelope, error) {
	return cq.Envelope{}, cq.ErrEnvelopeNotFound
}
```

Poller outline (where receipt handles are wired for `Ack`/`Reschedule`):

```go
for _, msg := range messages {
	var env cq.Envelope
	if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &env); err != nil {
		continue
	}

	// Initial receipt-handle mapping happens here.
	store.RememberReceipt(env.ID, aws.ToString(msg.ReceiptHandle))

	factory, ok := registry.FactoryFor(env.Type)
	if !ok {
		continue // Unknown type... move to observability/DLQ path.
	}

	job, err := factory(env)
	if err != nil {
		continue // Bad payload decode or factory error.
	}

	queue.Enqueue(job)
	// During execution, cq invokes store.Claim/Ack/Nack/Reschedule as needed.
}
```

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
	env.Payload = append([]byte(nil), env.Payload...) // Keep payload bytes owned by this write path.
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
		log.Printf("ship shipment_id=%s via=%s", payload.ShipmentID, payload.Carrier)
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

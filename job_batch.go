package cq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Sentinel errors returned by WithBatch.
var (
	ErrBatchInitFailed   = errors.New("cq: batch init failed")
	ErrBatchRecordFailed = errors.New("cq: batch record failed")
	ErrBatchLoadFailed   = errors.New("cq: batch load failed")
	ErrBatchNotFound     = errors.New("cq: batch not found")
)

// BatchRecord is the persisted state of a batch.
type BatchRecord struct {
	Total     int      // Total number of jobs in the batch.
	Completed int      // Number of jobs that have finished (success or failure).
	Failed    int      // Number of jobs that have failed.
	Errors    []string // Error messages from failed jobs (string form for serialization).
	Done      bool     // True once Completed == Total.
}

// BatchStore persists batch progress. Implementations must make RecordResult
// atomic — it is called concurrently from worker goroutines.
type BatchStore interface {
	// Init creates a record for batchID with total jobs. Idempotent.
	Init(ctx context.Context, batchID string, total int) error
	// RecordResult atomically increments counters and appends jobErr (if non-nil).
	// justCompleted is true exactly once: the call that flipped Completed to Total.
	RecordResult(ctx context.Context, batchID string, jobErr error) (rec BatchRecord, justCompleted bool, err error)
	// Load returns the current record for batchID.
	Load(ctx context.Context, batchID string) (rec BatchRecord, exists bool, err error)
	// Delete removes the record for batchID.
	Delete(ctx context.Context, batchID string) error
}

// BatchState is the in-process handle for a batch. Use Snapshot, Done, and
// IsDone to query progress and wait for completion. Atomic counters and the
// Errors slice remain exposed for in-memory introspection.
type BatchState struct {
	id    string     // Batch ID.
	store BatchStore // Batch store (default is in-memory).

	TotalJobs     int32        // Total number of jobs in the batch.
	CompletedJobs atomic.Int32 // Number of jobs that have finished (success or failure).
	FailedJobs    atomic.Int32 // Number of jobs that have failed.

	onComplete func([]error)              // Callback fired once when all jobs finish.
	onProgress func(completed, total int) // Callback fired after each completed job.

	errorsMut sync.Mutex
	Errors    []error // Errors from failed jobs (in-process view).

	doneOnce sync.Once     // Ensures onComplete is called at most once.
	doneCh   chan struct{} // Channel closed when batch is complete.

	initOnce sync.Once // Ensures the store is initialized at most once.
	initErr  error     // Cached init error (set inside initOnce, read after).
}

// ID returns the batch identifier.
func (s *BatchState) ID() string {
	return s.id
}

// Done returns a channel closed when this process observes batch completion.
// Cross-process consumers should poll Snapshot against the shared store.
func (s *BatchState) Done() <-chan struct{} {
	return s.doneCh
}

// Snapshot returns the current persisted state of the batch. Returns
// ErrBatchNotFound if the record has been deleted or never existed.
func (s *BatchState) Snapshot(ctx context.Context) (BatchRecord, error) {
	rec, exists, err := s.store.Load(ctx, s.id)
	if err != nil {
		return BatchRecord{}, fmt.Errorf("%w: %w", ErrBatchLoadFailed, err)
	}
	if !exists {
		return BatchRecord{}, ErrBatchNotFound
	}
	return rec, nil
}

// IsDone reports whether the batch has finished. Returns false (without error)
// if the batch record does not exist.
func (s *BatchState) IsDone(ctx context.Context) (bool, error) {
	rec, err := s.Snapshot(ctx)
	if errors.Is(err, ErrBatchNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return rec.Done, nil
}

// fireComplete runs the onComplete callback (if any) and closes Done(). It is
// safe to call more than once: the underlying sync.Once ensures the callback
// fires at most once even if a buggy BatchStore returns justCompleted twice.
func (s *BatchState) fireComplete() {
	s.doneOnce.Do(func() {
		if s.onComplete != nil {
			s.onComplete(s.snapshotErrors())
		}
		close(s.doneCh)
	})
}

// recordLocal atomically increments CompletedJobs (always) and FailedJobs
// (when err is non-nil), and appends err to the local Errors slice. Returns
// the new CompletedJobs count.
func (s *BatchState) recordLocal(err error) (completed int32) {
	if err != nil {
		s.FailedJobs.Add(1)
		s.errorsMut.Lock()
		s.Errors = append(s.Errors, err)
		s.errorsMut.Unlock()
	}
	return s.CompletedJobs.Add(1)
}

// snapshotErrors returns a copy of the Errors slice.
func (s *BatchState) snapshotErrors() []error {
	s.errorsMut.Lock()
	defer s.errorsMut.Unlock()
	out := make([]error, len(s.Errors))
	copy(out, s.Errors)
	return out
}

// BatchOption configures WithBatch behavior.
type BatchOption func(*batchConfig)

// batchConfig configures WithBatch behavior.
type batchConfig struct {
	id          string                     // Batch ID.
	namespace   string                     // Namespace prefix for the batch ID.
	idGenerator IDGenerator                // Optional custom batch ID generator.
	store       BatchStore                 // Batch store (default is in-memory).
	onComplete  func([]error)              // Callback fired once when all jobs finish.
	onProgress  func(completed, total int) // Callback fired after each completed job.
}

// WithBatchID sets the batch identifier. If omitted, an ID is auto-generated.
func WithBatchID(id string) BatchOption {
	return func(c *batchConfig) {
		c.id = strings.TrimSpace(id)
	}
}

// WithBatchNamespace prefixes the batch ID with namespace + ":".
func WithBatchNamespace(ns string) BatchOption {
	return func(c *batchConfig) {
		c.namespace = strings.TrimSpace(ns)
	}
}

// WithBatchStore overrides the default in-memory store.
func WithBatchStore(s BatchStore) BatchOption {
	return func(c *batchConfig) {
		if s != nil {
			c.store = s
		}
	}
}

// WithBatchOnComplete sets the callback fired once when all jobs finish.
func WithBatchOnComplete(fn func([]error)) BatchOption {
	return func(c *batchConfig) {
		c.onComplete = fn
	}
}

// WithBatchOnProgress sets the callback fired after each completed job.
func WithBatchOnProgress(fn func(completed, total int)) BatchOption {
	return func(c *batchConfig) {
		c.onProgress = fn
	}
}

// WithBatchIDGenerator sets a custom generator for batch IDs.
// Returning an empty ID falls back to the default atomic counter.
// Ignored when WithBatchID is also provided.
func WithBatchIDGenerator(gen IDGenerator) BatchOption {
	return func(c *batchConfig) {
		c.idGenerator = gen
	}
}

// batchIDCounter is the default fallback counter for batch IDs.
var batchIDCounter atomic.Uint64

// nextBatchID resolves the batch ID using, in order:
//   - cfg.id if set.
//   - cfg.idGenerator() if set and returns non-empty.
//   - the default atomic counter.
func nextBatchID(cfg *batchConfig) string {
	if cfg.id != "" {
		return cfg.id
	}
	if cfg.idGenerator != nil {
		if id := cfg.idGenerator(); id != "" {
			return id
		}
	}
	return strconv.FormatUint(batchIDCounter.Add(1), 10)
}

// WithBatch wraps jobs so they are tracked as a logical batch with shared
// progress state. Returns wrapped jobs and a BatchState handle for querying
// progress and waiting on completion.
//
// By default jobs are tracked through an in-memory store. Provide
// WithBatchStore to plug in durable storage for cross-process queries.
func WithBatch(jobs []Job, opts ...BatchOption) ([]Job, *BatchState) {
	if len(jobs) == 0 {
		return nil, nil
	}

	cfg := batchConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.store == nil {
		cfg.store = NewMemoryBatchStore()
	}

	id := nextBatchID(&cfg)
	if cfg.namespace != "" {
		id = cfg.namespace + ":" + id
	}

	total := len(jobs)
	state := &BatchState{
		id:         id,
		store:      cfg.store,
		TotalJobs:  int32(total),
		Errors:     make([]error, 0),
		onComplete: cfg.onComplete,
		onProgress: cfg.onProgress,
		doneCh:     make(chan struct{}),
	}

	wrapped := make([]Job, total)
	for i, job := range jobs {
		wrapped[i] = func(ojob Job) Job {
			return func(ctx context.Context) error {
				// Lazy, one-shot init using the worker's context so cancellation,
				// deadlines, and tracing scope are honored.
				state.initOnce.Do(func() {
					if err := cfg.store.Init(ctx, id, total); err != nil {
						state.initErr = fmt.Errorf("%w: %w", ErrBatchInitFailed, err)
					}
				})

				if state.initErr != nil {
					// Init failed: every job in the batch short-circuits with the
					// cached error. Track completion locally so onComplete still fires.
					completed := state.recordLocal(state.initErr)
					if state.onProgress != nil {
						state.onProgress(int(completed), total)
					}
					if completed >= int32(total) {
						state.fireComplete()
					}
					return state.initErr
				}

				err := ojob(ctx)

				rec, justCompleted, recErr := cfg.store.RecordResult(ctx, id, err)
				if recErr != nil {
					recErr = fmt.Errorf("%w: %w", ErrBatchRecordFailed, recErr)
					if err == nil {
						err = recErr
					}
				}

				state.recordLocal(err)

				if state.onProgress != nil {
					state.onProgress(rec.Completed, rec.Total)
				}

				if justCompleted {
					state.fireComplete()
				}

				return err
			}
		}(job)
	}

	return wrapped, state
}

// MemoryBatchStore is an in-memory BatchStore implementation.
type MemoryBatchStore struct {
	mu      sync.Mutex
	records map[string]*BatchRecord
}

// NewMemoryBatchStore creates a new in-memory batch store.
func NewMemoryBatchStore() *MemoryBatchStore {
	return &MemoryBatchStore{records: make(map[string]*BatchRecord)}
}

// Init creates a record for batchID. Idempotent: a second call for the same
// ID is a no-op (existing totals are preserved).
func (m *MemoryBatchStore) Init(_ context.Context, batchID string, total int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[batchID]; ok {
		return nil
	}
	m.records[batchID] = &BatchRecord{Total: total, Errors: make([]string, 0)}
	return nil
}

// RecordResult atomically increments counters for batchID.
func (m *MemoryBatchStore) RecordResult(_ context.Context, batchID string, jobErr error) (BatchRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.records[batchID]
	if !ok {
		return BatchRecord{}, false, fmt.Errorf("batch %q not initialized", batchID)
	}

	wasDone := rec.Done
	rec.Completed++
	if jobErr != nil {
		rec.Failed++
		rec.Errors = append(rec.Errors, jobErr.Error())
	}
	justCompleted := false
	if !wasDone && rec.Completed >= rec.Total {
		rec.Done = true
		justCompleted = true
	}

	return cloneBatchRecord(*rec), justCompleted, nil
}

// Load returns the current record for batchID.
func (m *MemoryBatchStore) Load(_ context.Context, batchID string) (BatchRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[batchID]
	if !ok {
		return BatchRecord{}, false, nil
	}
	return cloneBatchRecord(*rec), true, nil
}

// Delete removes a batch record.
func (m *MemoryBatchStore) Delete(_ context.Context, batchID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, batchID)
	return nil
}

// cloneBatchRecord returns a copy of the BatchRecord.
func cloneBatchRecord(r BatchRecord) BatchRecord {
	if len(r.Errors) > 0 {
		errs := make([]string, len(r.Errors))
		copy(errs, r.Errors)
		r.Errors = errs
	}
	return r
}

package cq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// submissionPending indicates an accepted submission waiting to execute.
	submissionPending uint32 = iota
	// submissionRunning indicates a submission currently executing.
	submissionRunning
	// submissionCompleting indicates a submission publishing its terminal result.
	submissionCompleting
	// submissionDone indicates a submission with an available terminal result.
	submissionDone
)

// ErrJobAbandoned is returned when an accepted job is abandoned before execution.
var ErrJobAbandoned = errors.New("cq: job abandoned before execution")

// JobResult is the terminal result of one accepted submission.
type JobResult struct {
	Meta       JobMeta
	StartedAt  time.Time
	FinishedAt time.Time
	Err        error
}

// Duration returns the execution duration. It returns zero before execution starts.
func (r JobResult) Duration() time.Duration {
	if r.StartedAt.IsZero() || r.FinishedAt.IsZero() {
		return 0
	}
	return r.FinishedAt.Sub(r.StartedAt)
}

// JobHandle tracks one accepted queue submission.
type JobHandle struct {
	meta JobMeta

	state atomic.Uint32
	done  chan struct{}

	mu     sync.RWMutex
	result JobResult
}

// newJobHandle creates a pending handle for one submission.
func newJobHandle(meta JobMeta) *JobHandle {
	meta = cloneJobMeta(meta)
	return &JobHandle{
		meta: meta,
		done: make(chan struct{}),
	}
}

// ID returns the submission's job ID.
func (h *JobHandle) ID() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.meta.ID
}

// Meta returns a copy of the submission's JobMeta.
func (h *JobHandle) Meta() JobMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return cloneJobMeta(h.meta)
}

// Done returns a channel closed when the submission reaches a terminal state.
func (h *JobHandle) Done() <-chan struct{} {
	return h.done
}

// Wait waits for submission completion or ctx cancellation.
// Cancelling ctx stops only the wait; it does not cancel the job.
func (h *JobHandle) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-h.done:
		result, _ := h.Result()
		return result.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Result returns the terminal result and whether the submission is complete.
func (h *JobHandle) Result() (JobResult, bool) {
	if h.state.Load() != submissionDone {
		return JobResult{}, false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := h.result
	result.Meta = cloneJobMeta(result.Meta)
	return result, true
}

// start transitions the handle from pending to running.
func (h *JobHandle) start(at time.Time) bool {
	if !h.state.CompareAndSwap(submissionPending, submissionRunning) {
		return false
	}
	h.mu.Lock()
	h.result.Meta = cloneJobMeta(h.meta)
	h.result.StartedAt = at
	h.mu.Unlock()
	return true
}

// setMeta updates metadata while the submission is pending.
func (h *JobHandle) setMeta(meta JobMeta) {
	h.mu.Lock()
	h.meta = cloneJobMeta(meta)
	h.mu.Unlock()
}

// finish records a running submission's terminal result.
func (h *JobHandle) finish(at time.Time, err error) bool {
	if !h.state.CompareAndSwap(submissionRunning, submissionCompleting) {
		return false
	}
	h.mu.Lock()
	h.result.FinishedAt = at
	h.result.Err = err
	h.mu.Unlock()
	h.state.Store(submissionDone)
	close(h.done)
	return true
}

// abandon completes a pending submission without executing it.
func (h *JobHandle) abandon() bool {
	return h.reject(ErrJobAbandoned)
}

// reject completes a pending submission without executing it.
func (h *JobHandle) reject(err error) bool {
	if !h.state.CompareAndSwap(submissionPending, submissionCompleting) {
		return false
	}
	now := time.Now()
	h.mu.Lock()
	h.result = JobResult{
		Meta:       cloneJobMeta(h.meta),
		FinishedAt: now,
		Err:        err,
	}
	h.mu.Unlock()
	h.state.Store(submissionDone)
	close(h.done)
	return true
}

// SubmitOption configures one submission.
type SubmitOption func(*submitConfig)

// submitConfig contains resolved options for one submission.
type submitConfig struct {
	id          string
	name        string
	attributes  map[string]string
	nonBlocking bool
}

// WithJobID sets the ID for one submission. Empty IDs use the queue generator.
func WithJobID(id string) SubmitOption {
	return func(cfg *submitConfig) {
		cfg.id = id
	}
}

// WithJobName sets a human-readable name for one submission.
func WithJobName(name string) SubmitOption {
	return func(cfg *submitConfig) {
		cfg.name = name
	}
}

// WithJobAttributes sets string attributes for one submission.
// Attributes are intended for correlation, observability, and external headers,
// not arbitrary job payloads.
func WithJobAttributes(attributes map[string]string) SubmitOption {
	return func(cfg *submitConfig) {
		cfg.attributes = cloneStringMap(attributes)
	}
}

// WithJobAttribute adds or replaces one string attribute for a submission.
func WithJobAttribute(key string, value string) SubmitOption {
	return func(cfg *submitConfig) {
		if cfg.attributes == nil {
			cfg.attributes = make(map[string]string)
		}
		cfg.attributes[key] = value
	}
}

// WithNonBlocking makes Submit return ErrQueueFull instead of waiting for capacity.
func WithNonBlocking() SubmitOption {
	return func(cfg *submitConfig) {
		cfg.nonBlocking = true
	}
}

// cloneStringMap returns an independent copy of values.
func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

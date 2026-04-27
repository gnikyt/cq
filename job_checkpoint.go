package cq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Sentinel errors returned by WithCheckpoint.
var (
	ErrCheckpointKeyUnavailable = errors.New("cq: checkpoint key unavailable")
	ErrCheckpointCheckFailed    = errors.New("cq: checkpoint check failed")
	ErrCheckpointMarkFailed     = errors.New("cq: checkpoint mark failed")
	ErrCheckpointDeleteFailed   = errors.New("cq: checkpoint delete failed")
)

// Checkpoint stores durable step state for workflow checkpointing.
type Checkpoint struct {
	Done bool
	Data []byte
}

// CheckpointStore persists workflow checkpoint records.
type CheckpointStore interface {
	// Load retrieves a checkpoint by key.
	Load(ctx context.Context, key string) (checkpoint Checkpoint, exists bool, err error)
	// Store writes a checkpoint by key.
	Store(ctx context.Context, key string, checkpoint Checkpoint) error
	// Delete removes a checkpoint key.
	Delete(ctx context.Context, key string) error
}

// CheckpointOption configures WithCheckpoint behavior.
type CheckpointOption func(*checkpointConfig)

// checkpointConfig configures WithCheckpoint behavior.
type checkpointConfig struct {
	namespace string
	// strict controls whether key/store failures return errors (true) or fall back to running the job (false).
	strict bool
	// keyFn resolves the checkpoint storage key for the current step execution.
	keyFn func(context.Context, string) (string, bool)
	// saveOnFailure persists checkpoint payload when job returns an error (stored as Done=false).
	saveOnFailure bool
	// deleteOnSuccess removes checkpoint record after successful execution instead of storing Done=true.
	deleteOnSuccess bool
}

// WithCheckpointNamespace prefixes checkpoint keys.
func WithCheckpointNamespace(namespace string) CheckpointOption {
	return func(cfg *checkpointConfig) {
		cfg.namespace = strings.TrimSpace(namespace)
	}
}

// WithCheckpointBestEffort tells WithCheckpoint to continue when checkpoint
// storage or key resolution fails. Strict mode is default.
func WithCheckpointBestEffort() CheckpointOption {
	return func(cfg *checkpointConfig) {
		cfg.strict = false
	}
}

// WithCheckpointSaveOnFailure persists checkpoint data even when job fails.
// Saved records are marked Done=false, allowing retries to resume from data.
func WithCheckpointSaveOnFailure() CheckpointOption {
	return func(cfg *checkpointConfig) {
		cfg.saveOnFailure = true
	}
}

// WithCheckpointDeleteOnSuccess deletes the checkpoint record after job success.
// This disables "done" skip semantics for future runs with the same key.
func WithCheckpointDeleteOnSuccess() CheckpointOption {
	return func(cfg *checkpointConfig) {
		cfg.deleteOnSuccess = true
	}
}

// WithCheckpointKeyFunc overrides how checkpoint keys are derived.
// The returned bool indicates whether a key was successfully resolved.
func WithCheckpointKeyFunc(fn func(context.Context, string) (string, bool)) CheckpointOption {
	return func(cfg *checkpointConfig) {
		if fn != nil {
			cfg.keyFn = fn
		}
	}
}

// WithCheckpoint runs job only if the checkpoint for step has not been marked.
// On successful completion it marks the step as done. If the same checkpoint key
// is observed again (for example: on retries), the wrapped job is skipped.
//
// The default key is "<job_id>:<step>" using JobMeta ID from context.
// If no key can be derived:
//   - Strict mode (default): Returns ErrCheckpointKeyUnavailable.
//   - Best-effort mode: Executes job without checkpointing.
func WithCheckpoint(job Job, step string, store CheckpointStore, opts ...CheckpointOption) Job {
	if store == nil || strings.TrimSpace(step) == "" {
		return job // No store or step... run job normally.
	}

	cfg := checkpointConfig{
		strict: true,
		keyFn:  defaultCheckpointKey,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return func(ctx context.Context) error {
		key, ok := cfg.keyFn(ctx, step)
		if !ok || strings.TrimSpace(key) == "" {
			if cfg.strict {
				return ErrCheckpointKeyUnavailable // Key unavailable in strict mode... fail.
			}
			return job(ctx) // Key unavailable in best-effort mode... run job normally.
		}
		if cfg.namespace != "" {
			key = cfg.namespace + ":" + key
		}

		cp, exists, err := store.Load(ctx, key)
		if err != nil {
			if cfg.strict {
				return fmt.Errorf("%w: %w", ErrCheckpointCheckFailed, err) // Check failed in strict mode... fail.
			}
			return job(ctx) // Check failed in best-effort mode... run job normally.
		}
		if exists && cp.Done {
			return nil // Key is done... skip job.
		}

		// Create a new checkpoint state for the job.
		state := &checkpointState{}
		if exists {
			state.data = cloneBytes(cp.Data)
		}
		jobCtx := contextWithCheckpointState(ctx, state)

		if err := job(jobCtx); err != nil {
			// Job failed... handle checkpoint persistence.
			if cfg.saveOnFailure {
				setErr := store.Store(ctx, key, Checkpoint{
					Done: false,
					Data: cloneBytes(state.data),
				})
				if setErr != nil && cfg.strict {
					return fmt.Errorf("%w: %w", ErrCheckpointMarkFailed, setErr)
				}
			}
			return err
		}

		if cfg.deleteOnSuccess {
			// Delete the checkpoint record after successful execution.
			if err := store.Delete(ctx, key); err != nil && cfg.strict {
				return fmt.Errorf("%w: %w", ErrCheckpointDeleteFailed, err)
			}
			return nil
		}

		// Store the checkpoint record after successful execution.
		if err := store.Store(ctx, key, Checkpoint{
			Done: true,
			Data: cloneBytes(state.data),
		}); err != nil && cfg.strict {
			return fmt.Errorf("%w: %w", ErrCheckpointMarkFailed, err)
		}
		return nil
	}
}

// checkpointStateKey is the context key for the checkpoint state.
type checkpointStateKey struct{}

// checkpointState is the state for the checkpoint.
type checkpointState struct {
	data []byte
}

// contextWithCheckpointState adds the checkpoint state to the context.
func contextWithCheckpointState(ctx context.Context, state *checkpointState) context.Context {
	return context.WithValue(ctx, checkpointStateKey{}, state)
}

// CheckpointDataFromContext returns checkpoint payload loaded for this step.
// Returns nil when no checkpoint payload is available.
func CheckpointDataFromContext(ctx context.Context) []byte {
	state, ok := ctx.Value(checkpointStateKey{}).(*checkpointState)
	if !ok || state == nil {
		return nil
	}
	return cloneBytes(state.data)
}

// SetCheckpointData updates checkpoint payload for the current step execution.
// Returns false when no checkpoint context is available.
func SetCheckpointData(ctx context.Context, data []byte) bool {
	state, ok := ctx.Value(checkpointStateKey{}).(*checkpointState)
	if !ok || state == nil {
		return false
	}
	state.data = cloneBytes(data)
	return true
}

// CheckpointDataAsJSON decodes checkpoint payload data as JSON into T.
// Returns ok=false when no checkpoint payload is available.
func CheckpointDataAsJSON[T any](ctx context.Context) (value T, ok bool, err error) {
	raw := CheckpointDataFromContext(ctx)
	if len(raw) == 0 {
		return value, false, nil
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return value, false, err
	}
	return value, true, nil
}

// SetCheckpointDataAsJSON JSON-encodes value and stores it as checkpoint payload data.
// Returns ok=false when no checkpoint context is available.
func SetCheckpointDataAsJSON[T any](ctx context.Context, value T) (ok bool, err error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	return SetCheckpointData(ctx, raw), nil
}

// defaultCheckpointKey derives a checkpoint key from the job ID and step.
func defaultCheckpointKey(ctx context.Context, step string) (string, bool) {
	meta := MetaFromContext(ctx)
	if meta.ID == "" {
		return "", false
	}
	return meta.ID + ":" + step, true
}

// MemoryCheckpointStore is an in-memory CheckpointStore implementation.
type MemoryCheckpointStore struct {
	checkpoints sync.Map
}

// NewMemoryCheckpointStore creates a new in-memory checkpoint store.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{}
}

// Load retrieves a checkpoint by key.
func (m *MemoryCheckpointStore) Load(_ context.Context, key string) (Checkpoint, bool, error) {
	raw, ok := m.checkpoints.Load(key)
	if !ok {
		return Checkpoint{}, false, nil
	}
	cp := raw.(Checkpoint)
	cp.Data = cloneBytes(cp.Data)
	return cp, true, nil
}

// Store writes a checkpoint by key.
func (m *MemoryCheckpointStore) Store(_ context.Context, key string, checkpoint Checkpoint) error {
	checkpoint.Data = cloneBytes(checkpoint.Data)
	m.checkpoints.Store(key, checkpoint)
	return nil
}

// Delete removes a checkpoint key.
func (m *MemoryCheckpointStore) Delete(_ context.Context, key string) error {
	m.checkpoints.Delete(key)
	return nil
}

// cloneBytes copies the bytes data and returns a new slice.
func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp
}

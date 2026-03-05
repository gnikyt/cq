package cq

import "context"

// PauseBehavior controls what enqueue does while queue execution is paused.
type PauseBehavior int

const (
	// PauseBuffer accepts enqueue and buffers jobs until resume.
	PauseBuffer PauseBehavior = iota
	// PauseReject rejects enqueue while paused.
	PauseReject
)

// PauseStore provides distributed pause state for one or more queue instances.
// Implementations can use Redis, SQL, etc.
type PauseStore interface {
	// IsPaused returns whether the queue identified by key is paused.
	IsPaused(ctx context.Context, key string) (bool, error)
	// SetPaused updates the pause state for the queue identified by key.
	SetPaused(ctx context.Context, key string, paused bool) error
}

package cq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

var (
	ErrUniqueContended         = errors.New("cq: unique contention detected")
	ErrWithoutOverlapContended = errors.New("cq: without overlap contention detected")
	ErrUniqueJobRequired       = errors.New("cq: unique job required")
	ErrUniqueLockerRequired    = errors.New("cq: unique locker required")
)

// defaultOverlapRetryTick controls how often WithoutOverlap retries acquisition.
const defaultOverlapRetryTick = 10 * time.Millisecond

// uniqueLockTokenCounter is a counter for unique lock tokens.
var uniqueLockTokenCounter atomic.Uint64

// uniqueOptions is a configuration for unique wrapper behavior.
type uniqueOptions struct {
	tokenGenerator func() string // Function to generate a unique lock token.
}

// overlapOptions configures WithoutOverlap behavior.
type overlapOptions struct {
	retryInterval time.Duration
}

// UniqueOption configures unique wrapper behavior.
type UniqueOption func(*uniqueOptions)

// OverlapOption configures WithoutOverlap behavior.
type OverlapOption func(*overlapOptions)

// uniqueContentionOrDiscard returns ErrUniqueContended under ContextWithContentionTry.
// Otherwise, it returns nil (quiet discard).
func uniqueContentionOrDiscard(ctx context.Context) error {
	if contentionTryFromContext(ctx) {
		return ErrUniqueContended
	}
	return nil
}

// WithUniqueTokenGenerator overrides unique lock token generation.
// Returning an empty token falls back to the default generator.
func WithUniqueTokenGenerator(gen func() string) UniqueOption {
	return func(opts *uniqueOptions) {
		opts.tokenGenerator = gen
	}
}

// WithOverlapRetryInterval sets how often WithoutOverlap retries lock acquisition.
// Non-positive intervals use the default.
func WithOverlapRetryInterval(interval time.Duration) OverlapOption {
	return func(opts *overlapOptions) {
		opts.retryInterval = interval
	}
}

// defaultUniqueTokenGenerator generates a random owner token suitable for cross-process usage.
func defaultUniqueTokenGenerator() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback should still be process-unique enough to avoid empty tokens.
		return fmt.Sprintf("%d-%d", time.Now().UnixNano(), uniqueLockTokenCounter.Add(1))
	}
	return hex.EncodeToString(b)
}

// resolveUniqueOptions resolves the unique options.
func resolveUniqueOptions(opts []UniqueOption) uniqueOptions {
	cfg := uniqueOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.tokenGenerator == nil {
		cfg.tokenGenerator = defaultUniqueTokenGenerator
	}
	return cfg
}

// resolveOverlapOptions resolves the overlap options.
func resolveOverlapOptions(opts []OverlapOption) overlapOptions {
	cfg := overlapOptions{
		retryInterval: defaultOverlapRetryTick,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.retryInterval <= 0 {
		cfg.retryInterval = defaultOverlapRetryTick
	}
	return cfg
}

// nextUniqueLockToken generates a new unique lock token.
func nextUniqueLockToken(gen func() string) string {
	if gen == nil {
		return defaultUniqueTokenGenerator()
	}
	if token := gen(); token != "" {
		return token
	}
	return defaultUniqueTokenGenerator()
}

// releaseLockByToken releases a lock by token.
func releaseLockByToken(ctx context.Context, locker Locker[struct{}], key string, token string) error {
	if renewableLocker, ok := locker.(RenewableLocker); ok && token != "" {
		_, err := renewableLocker.ReleaseIfOwner(ctx, key, token)
		return err
	}

	_, err := locker.Release(ctx, key)
	return err
}

// WithoutOverlap ensures multiple jobs of a given key cannot run concurrently.
// When ctx carries contention-try mode (see ContextWithContentionTry),
// WithoutOverlap makes one acquisition attempt and returns
// ErrWithoutOverlapContended when the key is already locked.
// WithErrorOnContention sets this automatically when invoking the job.
func WithoutOverlap(job Job, key string, locker Locker[struct{}], opts ...OverlapOption) Job {
	cfg := resolveOverlapOptions(opts)
	return func(ctx context.Context) (err error) {
		if job == nil {
			return ErrUniqueJobRequired
		}
		if locker == nil {
			return ErrUniqueLockerRequired
		}
		if err := acquireOverlap(ctx, locker, key, contentionTryFromContext(ctx), cfg.retryInterval); err != nil {
			return err
		}

		defer func() {
			_, releaseErr := locker.Release(context.WithoutCancel(ctx), key)
			err = errors.Join(err, releaseErr)
		}()
		return job(ctx)
	}
}

// acquireOverlap waits for ownership unless tryOnly requests one attempt.
func acquireOverlap(ctx context.Context, locker Locker[struct{}], key string, tryOnly bool, retryInterval time.Duration) error {
	lock := LockValue[struct{}]{Value: struct{}{}}
	for {
		acquired, err := locker.Acquire(ctx, key, lock)
		if err != nil {
			return err
		}
		if acquired {
			return nil
		}
		if tryOnly {
			return ErrWithoutOverlapContended
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// WithUnique ensures only one job of a given key runs within a time window.
// If a job with the same key already ran within the duration (ut), duplicate
// jobs are discarded (returns nil). The lock is released when the job completes
// or when the duration expires, whichever comes first.
// If ut is zero, the lock persists until the job completes, ensuring only
// one instance can run at a time without any time-based constraint.
// For enforcing a fixed minimum time between executions regardless of job
// completion time, use WithUniqueWindow instead.
// Under WithErrorOnContention, duplicate runs return ErrUniqueContended so the
// caller can apply its own retry or routing policy.
func WithUnique(job Job, key string, ut time.Duration, locker ReadLocker[struct{}], opts ...UniqueOption) Job {
	cfg := resolveUniqueOptions(opts)
	return func(ctx context.Context) (err error) {
		if job == nil {
			return ErrUniqueJobRequired
		}
		if locker == nil {
			return ErrUniqueLockerRequired
		}

		lock, exists, err := locker.Get(ctx, key)
		if err != nil {
			return err
		}
		if exists {
			if !lock.IsExpired() {
				return uniqueContentionOrDiscard(ctx)
			}
			// Lock exists, but is expired, release it. In this event, the job may have
			// not been processed yet, took too long to complete, etc.
			if err := releaseLockByToken(ctx, locker, key, lock.Token); err != nil {
				return err
			}
		}

		// Lock either does not exist or was released... acquire a new lock.
		var es struct{}
		var expiresAt time.Time
		renewableLocker, renewable := locker.(RenewableLocker)
		token := ""
		if renewable {
			token = nextUniqueLockToken(cfg.tokenGenerator)
		}
		if ut == 0 {
			// Zero duration means no expiration (lock until job completes).
			expiresAt = time.Time{}
		} else {
			// Append duration to now.
			expiresAt = time.Now().Add(ut)
		}
		acquired, err := locker.Acquire(ctx, key, LockValue[struct{}]{
			ExpiresAt: expiresAt,
			Value:     es,
			Token:     token,
		})
		if err != nil {
			return err
		}
		if !acquired {
			return uniqueContentionOrDiscard(ctx) // Lock acquisition failed, return the contention error.
		}

		if renewable && ut > 0 {
			ctx = contextWithLockTouchRequester(ctx, func(ttl time.Duration) error {
				if ttl <= 0 {
					ttl = ut
				}
				renewed, err := renewableLocker.Touch(ctx, key, token, time.Now().Add(ttl))
				if err != nil {
					return err
				}
				if !renewed {
					return ErrUniqueLeaseLost
				}
				return nil
			})
		}

		defer func() {
			err = errors.Join(err, releaseLockByToken(context.WithoutCancel(ctx), locker, key, token))
		}()
		return job(ctx)
	}
}

// WithUniqueWindow ensures a job can only run once within a fixed time window,
// regardless of how quickly the job completes. Unlike WithUnique, the lock is
// not released when the job completes... instead, it persists for the full duration.
// This guarantees a minimum time gap between executions.
// Under WithErrorOnContention, duplicate runs return ErrUniqueContended so the
// caller can apply its own retry or routing policy.
func WithUniqueWindow(job Job, key string, window time.Duration, locker ReadLocker[struct{}], opts ...UniqueOption) Job {
	cfg := resolveUniqueOptions(opts)
	return func(ctx context.Context) error {
		if job == nil {
			return ErrUniqueJobRequired
		}
		if locker == nil {
			return ErrUniqueLockerRequired
		}

		lock, exists, err := locker.Get(ctx, key)
		if err != nil {
			return err
		}
		if exists {
			if !lock.IsExpired() {
				return uniqueContentionOrDiscard(ctx)
			}
			// Lock exists, but is expired, release it. In this event, the job may have
			// not been processed yet, took too long to complete, etc.
			if err := releaseLockByToken(ctx, locker, key, lock.Token); err != nil {
				return err
			}
		}

		renewableLocker, renewable := locker.(RenewableLocker)
		token := ""
		if renewable {
			token = nextUniqueLockToken(cfg.tokenGenerator)
		}

		// Acquire lock for the full window duration.
		acquired, err := locker.Acquire(ctx, key, LockValue[struct{}]{
			ExpiresAt: time.Now().Add(window),
			Value:     struct{}{},
			Token:     token,
		})
		if err != nil {
			return err
		}
		if !acquired {
			return uniqueContentionOrDiscard(ctx) // Lock acquisition failed, return the contention error.
		}

		if renewable && window > 0 {
			ctx = contextWithLockTouchRequester(ctx, func(ttl time.Duration) error {
				if ttl <= 0 {
					ttl = window
				}
				renewed, err := renewableLocker.Touch(ctx, key, token, time.Now().Add(ttl))
				if err != nil {
					return err
				}
				if !renewed {
					return ErrUniqueLeaseLost
				}
				return nil
			})
		}

		return job(ctx)
	}
}

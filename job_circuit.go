package cq

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// circuitState represents the circuit breaker state.
type circuitState string

const (
	circuitClosed   circuitState = "closed"
	circuitOpen     circuitState = "open"
	circuitHalfOpen circuitState = "half-open"
)

// CircuitBreaker tracks failures and opens the circuit after threshold is reached.
type CircuitBreaker struct {
	threshold int           // Failures before opening.
	cooldown  time.Duration // How long to stay open.
	halfOpen  bool          // Enable half-open state (default true).

	mu        sync.Mutex
	failures  int          // Consecutive failure count.
	openUntil time.Time    // When the circuit can transition to half-open.
	state     circuitState // Current state.
}

// NewCircuitBreaker creates a circuit breaker with half-open enabled by default.
// threshold is the number of consecutive failures before opening (minimum 1).
// cooldown is how long the circuit stays open before allowing a test request.
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold < 1 {
		threshold = 1
	}
	return &CircuitBreaker{
		threshold: threshold,
		cooldown:  cooldown,
		halfOpen:  true,
		state:     circuitClosed,
	}
}

// SetHalfOpen enables or disables half-open behavior.
// When disabled, circuit closes fully after cooldown (all jobs allowed through).
// When enabled (default), only one job is allowed through to test recovery.
func (cb *CircuitBreaker) SetHalfOpen(enabled bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.halfOpen = enabled
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Check if open state has expired.
	if cb.state == circuitOpen && time.Now().After(cb.openUntil) {
		if cb.halfOpen {
			return string(circuitHalfOpen)
		}
		return string(circuitClosed)
	}
	return string(cb.state)
}

// IsOpen returns true if the circuit is currently open (rejecting calls).
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state == circuitOpen && time.Now().Before(cb.openUntil)
}

// tryAcquire attempts to acquire permission to execute.
// Returns true if the job should run, false if circuit is open.
func (cb *CircuitBreaker) tryAcquire() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case circuitClosed:
		return true // Allowed to run.
	case circuitOpen:
		if time.Now().After(cb.openUntil) {
			if cb.halfOpen {
				// Transition to half-open, allow one request.
				cb.state = circuitHalfOpen
				return true
			}

			// Half-open disabled, just close the circuit.
			cb.state = circuitClosed
			cb.failures = 0
			return true
		}
		return false
	case circuitHalfOpen:
		return false // Already testing, reject others.
	}
	return false // Should never happen.
}

// RecordSuccess resets the failure count and closes the circuit.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = circuitClosed
}

// RecordFailure increments failures and opens circuit if threshold reached.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	if cb.failures >= cb.threshold || cb.state == circuitHalfOpen {
		cb.state = circuitOpen
		cb.openUntil = time.Now().Add(cb.cooldown)
	}
}

// WithCircuitBreaker wraps a job with circuit breaker protection.
// Returns ErrCircuitOpen immediately if the circuit is open.
func WithCircuitBreaker(job Job, cb *CircuitBreaker) Job {
	return func(ctx context.Context) error {
		if !cb.tryAcquire() {
			return ErrCircuitOpen
		}

		err := job(ctx)
		if err != nil {
			cb.RecordFailure()
		} else {
			cb.RecordSuccess()
		}
		return err
	}
}

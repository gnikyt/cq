package cq

// Middleware decorates a job before execution.
// Useful for queue-wide behavior like logging, metrics, tracing, or defaults
// rather than setting the same behavior for every job individually.
type Middleware func(Job) Job

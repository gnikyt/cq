# Documentation

Complete reference documentation for cq.

## Getting Started

- [Main README](../README.md) - Quick start, installation, and overview
- [Common Recipes](../README.md#common-recipes) - Practical examples to get you started

## Core Features

### [Job Wrappers](JOB_WRAPPERS.md)
Complete reference for all job wrappers including:
- Retry logic and backoff strategies
- Timeouts and deadlines
- Outcome handlers and tracing
- Rate limiting and circuit breakers
- Workflow composition (chains, pipelines, batches)
- Deduplication and overlap prevention

### [Envelope Persistence](ENVELOPE_PERSISTENCE.md)
Persist and recover jobs using envelope stores:
- Lifecycle callbacks and recovery APIs
- DLQ-only implementation example
- File-backed storage example
- DynamoDB implementation example

## Advanced Topics

### [Priority Queue](PRIORITY_QUEUE.md)
Weighted fair queuing with priority levels:
- Priority-based dispatch
- Custom weight configuration
- Graceful shutdown with drain

### [Scheduler](SCHEDULER.md)
Recurring and one-time job scheduling:
- Interval-based scheduling
- Time-based scheduling
- Cron-like behavior with external parsers

### [Custom Locker](CUSTOM_LOCKER.md)
Distributed lock implementations for `WithUnique` and `WithoutOverlap`:
- Locker interface specification
- Redis implementation example
- SQLite implementation example

### [Queue Options](QUEUE_OPTIONS.md)
Queue construction and runtime options:
- Worker idle cleanup tuning
- Context and cancellation strategies
- Panic handling
- Envelope store integration
- Custom job ID generation

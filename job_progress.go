package cq

import (
	"context"
	"sync"
)

// ProgressReporter receives progress updates from running jobs.
// Reporting is best-effort observability: implementations must not block
// for long and must be safe for concurrent use.
type ProgressReporter interface {
	// ReportProgress is called on every SetProgress from a job wrapped
	// with WithProgress. meta identifies the reporting job.
	ReportProgress(ctx context.Context, meta JobMeta, progress Progress)
}

// Progress describes how far along a running job is.
// Every field is optional: report counts (semantics of Completed/Total are
// job-defined... rows, bytes, steps), a stage label only, or both.
type Progress struct {
	Completed int64  // Units of work completed so far.
	Total     int64  // Total units of work expected... zero when unknown.
	Stage     string // Optional human-readable stage label.
}

// Fraction returns completion as a value between 0 and 1.
// ok is false when Total is unknown (zero or negative), such as stage-only
// progress, letting observers distinguish "unknown" from "0% done".
func (p Progress) Fraction() (fraction float64, ok bool) {
	if p.Total <= 0 {
		return 0, false
	}
	f := float64(p.Completed) / float64(p.Total)
	if f < 0 {
		return 0, true
	}
	if f > 1 {
		return 1, true
	}
	return f, true
}

// progressStateKey is the context key for the progress state.
type progressStateKey struct{}

// progressState is the mutable progress for one job execution.
type progressState struct {
	mu     sync.RWMutex
	p      Progress
	set    bool
	report func(context.Context, Progress)
}

// setProgress stores the latest progress and pushes it to the reporter.
func (s *progressState) setProgress(ctx context.Context, p Progress) {
	s.mu.Lock()
	s.p = p
	s.set = true
	report := s.report
	s.mu.Unlock()
	if report != nil {
		report(ctx, p)
	}
}

// progress returns the latest progress and whether one was set.
func (s *progressState) progress() (Progress, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.p, s.set
}

// WithProgress lets job report incremental progress via SetProgress.
// Each update is pushed to reporter (when non-nil) together with the job's
// metadata, and the latest update remains readable in the job's context via
// ProgressFromContext. Reporting is best-effort and never affects the job's
// outcome.
func WithProgress(job Job, reporter ProgressReporter) Job {
	return func(ctx context.Context) error {
		state := &progressState{}
		if reporter != nil {
			meta := MetaFromContext(ctx)
			state.report = func(reportCtx context.Context, p Progress) {
				reporter.ReportProgress(reportCtx, meta, p)
			}
		}
		return job(context.WithValue(ctx, progressStateKey{}, state))
	}
}

// SetProgress records progress for the current job execution.
// Returns false when no progress context is available (the job is not
// wrapped with WithProgress).
func SetProgress(ctx context.Context, progress Progress) bool {
	state, ok := ctx.Value(progressStateKey{}).(*progressState)
	if !ok || state == nil {
		return false
	}
	state.setProgress(ctx, progress)
	return true
}

// ProgressFromContext returns the latest progress recorded for the current
// job execution. ok is false when no progress was recorded or no progress
// context is available.
func ProgressFromContext(ctx context.Context) (progress Progress, ok bool) {
	state, isSet := ctx.Value(progressStateKey{}).(*progressState)
	if !isSet || state == nil {
		return Progress{}, false
	}
	return state.progress()
}

// MemoryProgressReporter is an in-memory ProgressReporter implementation.
// It keeps the latest progress per job ID for pull-based observation
// (dashboards, ops endpoints). Entries persist until Delete is called.
type MemoryProgressReporter struct {
	progresses sync.Map
}

// NewMemoryProgressReporter creates a new in-memory progress reporter.
func NewMemoryProgressReporter() *MemoryProgressReporter {
	return &MemoryProgressReporter{}
}

// ReportProgress stores the latest progress by job ID.
// Updates without a job ID in meta are ignored.
func (m *MemoryProgressReporter) ReportProgress(_ context.Context, meta JobMeta, progress Progress) {
	if meta.ID == "" {
		return
	}
	m.progresses.Store(meta.ID, progress)
}

// Progress returns the latest progress reported for a job ID.
func (m *MemoryProgressReporter) Progress(id string) (Progress, bool) {
	raw, ok := m.progresses.Load(id)
	if !ok {
		return Progress{}, false
	}
	return raw.(Progress), true
}

// Delete removes the progress entry for a job ID.
func (m *MemoryProgressReporter) Delete(id string) {
	m.progresses.Delete(id)
}

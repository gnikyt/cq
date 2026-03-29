package cq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDependsOnNoDeps(t *testing.T) {
	// Zero deps: wrapper is a no-op, job runs immediately.
	executed := false
	job := func(ctx context.Context) error {
		executed = true
		return nil
	}

	wrapped := WithDependsOn(job)
	err := wrapped(context.Background())

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !executed {
		t.Error("job should have executed")
	}
}

func TestDependsOnAllSucceed(t *testing.T) {
	// All deps succeed, job should run.
	execOrder := []string{}

	dep1 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep1")
		return nil
	}
	dep2 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep2")
		return nil
	}
	job := func(ctx context.Context) error {
		execOrder = append(execOrder, "job")
		return nil
	}

	wrapped := WithDependsOn(
		job,
		Dep(dep1, DependencyFailCancel),
		Dep(dep2, DependencyFailContinue),
	)
	err := wrapped(context.Background())

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if len(execOrder) != 3 || execOrder[0] != "dep1" || execOrder[1] != "dep2" || execOrder[2] != "job" {
		t.Errorf("expected [dep1 dep2 job], got %v", execOrder)
	}
}

func TestDependsOnFailCancel(t *testing.T) {
	// Dep fails with DependencyFailCancel: downstream stops, job doesn't run.
	execOrder := []string{}
	depErr := errors.New("dep1 failed")

	dep1 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep1")
		return depErr
	}
	dep2 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep2")
		return nil
	}
	job := func(ctx context.Context) error {
		execOrder = append(execOrder, "job")
		return nil
	}

	wrapped := WithDependsOn(
		job,
		Dep(dep1, DependencyFailCancel),
		Dep(dep2, DependencyFailContinue),
	)
	err := wrapped(context.Background())

	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, ErrDependencyCancelled) {
		t.Errorf("expected ErrDependencyCancelled, got %v", err)
	}
	if !errors.Is(err, depErr) {
		t.Errorf("expected wrapped depErr, got %v", err)
	}
	// Only dep1 should have run; dep2 and job should not.
	if len(execOrder) != 1 || execOrder[0] != "dep1" {
		t.Errorf("expected [dep1], got %v", execOrder)
	}
}

func TestDependsOnFailSkip(t *testing.T) {
	// Dep fails with DependencyFailSkip: returns AsDiscard, job doesn't run.
	execOrder := []string{}
	depErr := errors.New("dep1 failed")

	dep1 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep1")
		return depErr
	}
	dep2 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep2")
		return nil
	}
	job := func(ctx context.Context) error {
		execOrder = append(execOrder, "job")
		return nil
	}

	wrapped := WithDependsOn(
		job,
		Dep(dep1, DependencyFailSkip),
		Dep(dep2, DependencyFailContinue),
	)
	err := wrapped(context.Background())

	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, ErrDiscard) {
		t.Errorf("expected ErrDiscard, got %v", err)
	}
	if !errors.Is(err, ErrDependencySkipped) {
		t.Errorf("expected ErrDependencySkipped, got %v", err)
	}
	// Only dep1 should have run.
	if len(execOrder) != 1 || execOrder[0] != "dep1" {
		t.Errorf("expected [dep1], got %v", execOrder)
	}
}

func TestDependsOnFailContinue(t *testing.T) {
	// Dep fails with DependencyFailContinue: failure ignored, next dep and job run.
	execOrder := []string{}
	depErr := errors.New("dep1 failed")

	dep1 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep1")
		return depErr
	}
	dep2 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep2")
		return nil
	}
	job := func(ctx context.Context) error {
		execOrder = append(execOrder, "job")
		return nil
	}

	wrapped := WithDependsOn(
		job,
		Dep(dep1, DependencyFailContinue),
		Dep(dep2, DependencyFailContinue),
	)
	err := wrapped(context.Background())

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	// All should have run despite dep1 failure.
	if len(execOrder) != 3 || execOrder[0] != "dep1" || execOrder[1] != "dep2" || execOrder[2] != "job" {
		t.Errorf("expected [dep1 dep2 job], got %v", execOrder)
	}
}

func TestDependsOnJobFails(t *testing.T) {
	// All deps succeed, but job itself fails.
	execOrder := []string{}
	jobErr := errors.New("job failed")

	dep1 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep1")
		return nil
	}
	job := func(ctx context.Context) error {
		execOrder = append(execOrder, "job")
		return jobErr
	}

	wrapped := WithDependsOn(
		job,
		Dep(dep1, DependencyFailCancel),
	)
	err := wrapped(context.Background())

	if err != jobErr {
		t.Errorf("expected jobErr, got %v", err)
	}
	if len(execOrder) != 2 || execOrder[0] != "dep1" || execOrder[1] != "job" {
		t.Errorf("expected [dep1 job], got %v", execOrder)
	}
}

func TestDependsOnContextCancellation(t *testing.T) {
	// Context cancelled during dep execution should propagate.
	ctx, cancel := context.WithCancel(context.Background())

	dep1 := func(ctxArg context.Context) error {
		cancel()                    // Cancel the context mid-dep.
		select {
		case <-ctxArg.Done():
			return ctxArg.Err()
		case <-time.After(100 * time.Millisecond):
			return errors.New("context not cancelled")
		}
	}
	job := func(ctxArg context.Context) error {
		return errors.New("job should not run")
	}

	wrapped := WithDependsOn(
		job,
		Dep(dep1, DependencyFailCancel),
	)
	err := wrapped(ctx)

	// The dep job's cancellation check should propagate the cancel error.
	// The wrapper itself returns whatever the dep returns.
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestDependsOnDepPanic(t *testing.T) {
	// Dep panics: should not crash the wrapper, panic propagates via recover (if any).
	// The wrapper itself doesn't recover panics; they bubble up to the caller/queue's panic handler.
	defer func() {
		if r := recover(); r != nil {
			// This is expected: panics in deps propagate naturally.
		}
	}()

	dep1 := func(ctx context.Context) error {
		panic("dep1 panicked")
	}
	job := func(ctx context.Context) error {
		return errors.New("should not reach")
	}

	wrapped := WithDependsOn(
		job,
		Dep(dep1, DependencyFailCancel),
	)

	// The panic will propagate up from dep1 through wrapped.
	// We defer a recover above to catch it.
	_ = wrapped(context.Background())
}

func TestDependsOnDepWrapped(t *testing.T) {
	// Dep wraps a job that fails on first call then succeeds; tests that
	// WithDependsOn defers to whatever the Dep job returns, regardless of wrapping.
	calls := 0
	jobExecuted := false

	// A manually "retried" dep: fails first call, succeeds second.
	dep1 := func(ctx context.Context) error {
		calls++
		if calls == 1 {
			return errors.New("first call fails")
		}
		return nil
	}
	// Simulate retry externally (not via WithRetry) to avoid using other wrappers.
	retryingDep := func(ctx context.Context) error {
		for {
			if err := dep1(ctx); err == nil {
				return nil
			}
		}
	}
	job := func(ctx context.Context) error {
		jobExecuted = true
		return nil
	}

	wrapped := WithDependsOn(job, Dep(retryingDep, DependencyFailCancel))
	err := wrapped(context.Background())

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !jobExecuted {
		t.Error("job should have executed after dep succeeded")
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestDependsOnDepGrouped(t *testing.T) {
	// Multiple jobs passed as a single Dep via a plain wrapper func.
	execOrder := []string{}

	group := func(ctx context.Context) error {
		execOrder = append(execOrder, "group-a")
		execOrder = append(execOrder, "group-b")
		return nil
	}
	dep2 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep2")
		return nil
	}
	job := func(ctx context.Context) error {
		execOrder = append(execOrder, "job")
		return nil
	}

	wrapped := WithDependsOn(
		job,
		Dep(group, DependencyFailCancel),
		Dep(dep2, DependencyFailContinue),
	)
	err := wrapped(context.Background())

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	expected := []string{"group-a", "group-b", "dep2", "job"}
	if len(execOrder) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, execOrder)
	}
	for i, v := range expected {
		if execOrder[i] != v {
			t.Errorf("index %d: expected %s, got %s", i, v, execOrder[i])
		}
	}
}

func TestDependsOnMultipleFailCancelDeps(t *testing.T) {
	// Multiple deps with DependencyFailCancel; first one to fail stops execution.
	execOrder := []string{}
	err1 := errors.New("dep1 failed")
	err2 := errors.New("dep2 failed")

	dep1 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep1")
		return err1
	}
	dep2 := func(ctx context.Context) error {
		execOrder = append(execOrder, "dep2")
		return err2
	}
	job := func(ctx context.Context) error {
		execOrder = append(execOrder, "job")
		return nil
	}

	wrapped := WithDependsOn(
		job,
		Dep(dep1, DependencyFailCancel),
		Dep(dep2, DependencyFailCancel),
	)
	err := wrapped(context.Background())

	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, ErrDependencyCancelled) {
		t.Errorf("expected ErrDependencyCancelled, got %v", err)
	}
	// Only dep1 runs; dep2 and job are skipped.
	if len(execOrder) != 1 || execOrder[0] != "dep1" {
		t.Errorf("expected [dep1], got %v", execOrder)
	}
}

func TestDependsOnMixedFailModes(t *testing.T) {
	// Multiple deps with mixed fail modes: first failure using Cancel/Skip stops execution.
	tests := []struct {
		name           string
		failMode1      DependencyFailMode
		failMode2      DependencyFailMode
		expectedErr    error
		expectedExecCount int
	}{
		{
			name:              "Cancel stops at first dep",
			failMode1:         DependencyFailCancel,
			failMode2:         DependencyFailContinue,
			expectedErr:       ErrDependencyCancelled,
			expectedExecCount: 1, // only dep1
		},
		{
			name:              "Skip stops at first dep",
			failMode1:         DependencyFailSkip,
			failMode2:         DependencyFailContinue,
			expectedErr:       ErrDiscard,
			expectedExecCount: 1, // only dep1
		},
		{
			name:              "Continue at first, Cancel at second",
			failMode1:         DependencyFailContinue,
			failMode2:         DependencyFailCancel,
			expectedErr:       ErrDependencyCancelled,
			expectedExecCount: 2, // dep1 and dep2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execOrder := []string{}
			dep1Err := errors.New("dep1 failed")
			dep2Err := errors.New("dep2 failed")

			dep1 := func(ctx context.Context) error {
				execOrder = append(execOrder, "dep1")
				return dep1Err
			}
			dep2 := func(ctx context.Context) error {
				execOrder = append(execOrder, "dep2")
				return dep2Err
			}
			job := func(ctx context.Context) error {
				execOrder = append(execOrder, "job")
				return nil
			}

			wrapped := WithDependsOn(
				job,
				Dep(dep1, tt.failMode1),
				Dep(dep2, tt.failMode2),
			)
			err := wrapped(context.Background())

			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("expected %v, got %v", tt.expectedErr, err)
			}
			if len(execOrder) != tt.expectedExecCount {
				t.Errorf("expected %d execs, got %d: %v", tt.expectedExecCount, len(execOrder), execOrder)
			}
		})
	}
}

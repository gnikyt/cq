package cq

import (
	"errors"
	"testing"
)

func TestAsDiscard(t *testing.T) {
	inner := errors.New("inner")
	err := AsDiscard(inner)
	if !errors.Is(err, ErrDiscard) {
		t.Error("errors.Is(err, ErrDiscard) should be true")
	}
	if errors.Unwrap(err) != inner {
		t.Errorf("Unwrap: got %v", errors.Unwrap(err))
	}
	if err.Error() == "" {
		t.Error("Error() should not be empty")
	}
}

func TestAsDiscard_nil(t *testing.T) {
	err := AsDiscard(nil)
	if !errors.Is(err, ErrDiscard) {
		t.Error("errors.Is(err, ErrDiscard) should be true")
	}
	if errors.Unwrap(err) != nil {
		t.Errorf("Unwrap(nil): got %v", errors.Unwrap(err))
	}
}

func TestAsRetryable(t *testing.T) {
	inner := errors.New("inner")
	err := AsRetryable(inner)
	if !errors.Is(err, ErrRetryable) {
		t.Error("errors.Is(err, ErrRetryable) should be true")
	}
	if errors.Unwrap(err) != inner {
		t.Errorf("Unwrap: got %v", errors.Unwrap(err))
	}
}

func TestAsPermanent(t *testing.T) {
	inner := errors.New("inner")
	err := AsPermanent(inner)
	if !errors.Is(err, ErrPermanent) {
		t.Error("errors.Is(err, ErrPermanent) should be true")
	}
	if errors.Unwrap(err) != inner {
		t.Errorf("Unwrap: got %v", errors.Unwrap(err))
	}
}

package cq

import (
	"context"
	"fmt"
)

// WithRecover converts job panics into returned errors.
// This allows panic cases to flow through wrappers such as WithResultHandler.
// Without this wrapper, panic handling is owned by the queue runtime.
func WithRecover(job Job) Job {
	return func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				meta := MetaFromContext(ctx)
				prefix := "job panic"
				if meta.ID != "" {
					prefix = fmt.Sprintf("job panic (id=%s)", meta.ID)
				}

				switch x := r.(type) {
				case string:
					err = fmt.Errorf("%s: %s", prefix, x)
				case error:
					err = fmt.Errorf("%s: %w", prefix, x)
				default:
					err = fmt.Errorf("%s: %v", prefix, x)
				}
			}
		}()
		return job(ctx)
	}
}

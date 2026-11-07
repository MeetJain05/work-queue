// Package handlers holds the code that actually runs each task type.
//
// The tasks are simulated (they sleep instead of calling a real SMTP server or
// image library) so the project stays focused on the queue itself. Each one
// respects ctx, so a job timeout stops it.
package handlers

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"WorkQueue/internal/task"
)

// Handler runs one job. Returning an error triggers a retry, unless the error
// is wrapped with Permanent.
type Handler func(ctx context.Context, job task.Job) error

// Task describes one supported task type.
type Task struct {
	// Validate checks the payload when the job is submitted, so bad requests
	// get a 400 from the producer instead of failing later in a worker.
	Validate func(payload map[string]any) error
	Run      Handler
}

// Registry maps a task type name to its implementation.
type Registry map[string]Task

// Validate reports whether taskType is supported and its payload is valid.
func (r Registry) Validate(taskType string, payload map[string]any) error {
	t, ok := r[taskType]
	if !ok {
		return fmt.Errorf("unsupported task_type %q", taskType)
	}
	if t.Validate != nil {
		return t.Validate(payload)
	}
	return nil
}

// permanentError marks a failure that retrying cannot fix.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the worker sends the job straight to the dead-letter
// queue instead of retrying it (for example, invalid input).
func Permanent(err error) error { return permanentError{err} }

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Default returns the task types the worker supports.
func Default() Registry {
	return Registry{
		"send_email": {
			Validate: requireStrings("to", "subject"),
			Run: func(ctx context.Context, _ task.Job) error {
				return work(ctx, 200*time.Millisecond+rand.N(600*time.Millisecond)) // SMTP round trip
			},
		},
		"resize_image": {
			Validate: validateResize,
			Run: func(ctx context.Context, job task.Job) error {
				if err := validateResize(job.Payload); err != nil {
					return Permanent(err)
				}
				return work(ctx, 300*time.Millisecond)
			},
		},
		"generate_pdf": {
			Run: func(ctx context.Context, _ task.Job) error {
				return work(ctx, 100*time.Millisecond)
			},
		},
		// flaky_task fails at random so you can watch retries, backoff and the
		// dead-letter queue. payload.fail_rate is between 0 and 1 (default 0.7).
		"flaky_task": {
			Run: func(ctx context.Context, job task.Job) error {
				rate, ok := job.Payload["fail_rate"].(float64)
				if !ok {
					rate = 0.7
				}
				if err := work(ctx, 50*time.Millisecond); err != nil {
					return err
				}
				if rand.Float64() < rate {
					return errors.New("simulated downstream timeout")
				}
				return nil
			},
		},
	}
}

// work simulates d of work and stops early if ctx is cancelled.
func work(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func requireStrings(keys ...string) func(map[string]any) error {
	return func(p map[string]any) error {
		for _, k := range keys {
			if s, ok := p[k].(string); !ok || s == "" {
				return fmt.Errorf("payload.%s is required", k)
			}
		}
		return nil
	}
}

func validateResize(p map[string]any) error {
	for _, k := range []string{"width", "height"} {
		v, ok := p[k].(float64) // JSON numbers decode as float64
		if !ok || v <= 0 || v > 10000 {
			return fmt.Errorf("payload.%s must be a number between 1 and 10000", k)
		}
	}
	return nil
}

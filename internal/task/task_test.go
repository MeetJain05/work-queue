package task

import (
	"errors"
	"testing"
	"time"
)

func TestNewJob(t *testing.T) {
	job := NewJob("send_email", map[string]any{"to": "user@example.com"}, 3)

	if job.JobID == "" {
		t.Fatal("job id should not be empty")
	}
	if job.Status != StatusPending {
		t.Fatalf("new job should be PENDING, got %s", job.Status)
	}
	if job.Attempts != 0 || job.MaxAttempts != 3 {
		t.Fatalf("unexpected attempts %d/%d", job.Attempts, job.MaxAttempts)
	}
	if job.CreatedAt.IsZero() {
		t.Fatal("created_at should be set")
	}
}

func TestNewJobDefaults(t *testing.T) {
	job := NewJob("generate_pdf", nil, 0)
	if job.Payload == nil {
		t.Fatal("nil payload should become an empty map")
	}
	if job.MaxAttempts != 1 {
		t.Fatalf("max attempts should be at least 1, got %d", job.MaxAttempts)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	job := NewJob("send_email", map[string]any{"to": "user@example.com"}, 3)
	job.MarkProcessing()

	data, err := Encode(job)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.JobID != job.JobID || got.Attempts != 1 || got.Status != StatusProcessing {
		t.Fatalf("round trip changed the job: %+v", got)
	}
	if got.Payload["to"] != "user@example.com" {
		t.Fatal("payload lost in round trip")
	}
}

func TestDecodeInvalid(t *testing.T) {
	if _, err := Decode([]byte("not json")); err == nil {
		t.Fatal("expected an error for invalid JSON")
	}
}

func TestLifecycle(t *testing.T) {
	job := NewJob("send_email", nil, 2)

	job.MarkProcessing()
	if job.Status != StatusProcessing || job.Attempts != 1 {
		t.Fatalf("expected PROCESSING attempt 1, got %s attempt %d", job.Status, job.Attempts)
	}
	if !job.CanRetry() {
		t.Fatal("attempt 1 of 2 should be retryable")
	}

	runAt := time.Now().Add(time.Second)
	job.MarkRetrying(errors.New("smtp timeout"), runAt)
	if job.Status != StatusRetrying || job.NextRetryAt == nil || job.ErrorMessage != "smtp timeout" {
		t.Fatalf("retry state not recorded: %+v", job)
	}

	job.MarkProcessing()
	if job.NextRetryAt != nil {
		t.Fatal("next_retry_at should clear when a new attempt starts")
	}
	if job.CanRetry() {
		t.Fatal("attempt 2 of 2 should not be retryable")
	}

	job.MarkFailed(errors.New("smtp timeout"))
	if job.Status != StatusFailed {
		t.Fatalf("expected FAILED, got %s", job.Status)
	}

	job.MarkCompleted()
	if job.ErrorMessage != "" {
		t.Fatal("completing should clear the error message")
	}
}

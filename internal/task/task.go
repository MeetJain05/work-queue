// Package task defines the Job model that moves through the queue.
package task

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

// JobStatus is the lifecycle state of a job.
type JobStatus string

const (
	StatusPending    JobStatus = "PENDING"    // in the stream, waiting for a worker
	StatusProcessing JobStatus = "PROCESSING" // a worker is running the handler
	StatusRetrying   JobStatus = "RETRYING"   // failed, waiting for its backoff delay
	StatusCompleted  JobStatus = "COMPLETED"  // handler succeeded
	StatusFailed     JobStatus = "FAILED"     // out of attempts, moved to the dead-letter queue
)

// Job is the unit of work stored in Redis.
type Job struct {
	JobID        string         `json:"job_id"`
	TaskType     string         `json:"task_type"`
	Payload      map[string]any `json:"payload"`
	Status       JobStatus      `json:"status"`
	Attempts     int            `json:"attempts"`
	MaxAttempts  int            `json:"max_attempts"`
	ErrorMessage string         `json:"error_message,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
	NextRetryAt  *time.Time     `json:"next_retry_at,omitempty"`
}

// NewJob creates a PENDING job with a random ID.
func NewJob(taskType string, payload map[string]any, maxAttempts int) Job {
	if payload == nil {
		payload = map[string]any{}
	}
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	now := time.Now().UTC()
	return Job{
		JobID:       newJobID(),
		TaskType:    taskType,
		Payload:     payload,
		Status:      StatusPending,
		MaxAttempts: maxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// MaxAttemptsFromEnv reads MAX_ATTEMPTS (total tries, including the first one).
func MaxAttemptsFromEnv() int {
	n, err := strconv.Atoi(os.Getenv("MAX_ATTEMPTS"))
	if err != nil || n <= 0 {
		return 3
	}
	return n
}

func (j *Job) set(status JobStatus) {
	j.Status = status
	j.UpdatedAt = time.Now().UTC()
}

// MarkProcessing records the start of a new attempt.
func (j *Job) MarkProcessing() {
	j.Attempts++
	j.NextRetryAt = nil
	j.set(StatusProcessing)
}

// MarkCompleted records a successful attempt.
func (j *Job) MarkCompleted() {
	j.ErrorMessage = ""
	j.set(StatusCompleted)
}

// MarkRetrying records a failed attempt that will be tried again at runAt.
func (j *Job) MarkRetrying(err error, runAt time.Time) {
	j.ErrorMessage = errString(err)
	runAt = runAt.UTC()
	j.NextRetryAt = &runAt
	j.set(StatusRetrying)
}

// MarkFailed records the final failed attempt.
func (j *Job) MarkFailed(err error) {
	j.ErrorMessage = errString(err)
	j.NextRetryAt = nil
	j.set(StatusFailed)
}

// CanRetry reports whether another attempt is allowed.
func (j *Job) CanRetry() bool { return j.Attempts < j.MaxAttempts }

// Encode serializes a job to JSON.
func Encode(j Job) ([]byte, error) { return json.Marshal(j) }

// Decode parses a job from JSON.
func Decode(data []byte) (Job, error) {
	var j Job
	if err := json.Unmarshal(data, &j); err != nil {
		return Job{}, fmt.Errorf("decode job: %w", err)
	}
	if j.Payload == nil {
		j.Payload = map[string]any{}
	}
	return j, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func newJobID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

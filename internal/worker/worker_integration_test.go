package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"WorkQueue/internal/handlers"
	"WorkQueue/internal/queue"
	"WorkQueue/internal/task"
	"WorkQueue/internal/testredis"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func fastConfig() Config {
	return Config{
		Workers:    4,
		Consumer:   "test",
		JobTimeout: 500 * time.Millisecond,
		ClaimIdle:  time.Second,
		RetryBase:  10 * time.Millisecond,
		RetryMax:   50 * time.Millisecond,
		Tick:       10 * time.Millisecond,
	}
}

func setup(t *testing.T, reg handlers.Registry, cfg Config) (*queue.Queue, *Pool) {
	t.Helper()
	rdb, prefix := testredis.Client(t)
	q := queue.New(rdb, prefix)
	p := NewPool(q, reg, cfg, quiet)
	return q, p
}

func start(t *testing.T, p *Pool) {
	t.Helper()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Stop)
}

func submit(t *testing.T, q *queue.Queue, typ string, maxAttempts int) task.Job {
	t.Helper()
	job := task.NewJob(typ, nil, maxAttempts)
	if err := q.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return job
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func status(q *queue.Queue, id string) task.JobStatus {
	j, _ := q.GetJob(context.Background(), id)
	return j.Status
}

func stats(q *queue.Queue) queue.Stats {
	s, _ := q.Stats(context.Background())
	return s
}

func registry(run handlers.Handler) handlers.Registry {
	return handlers.Registry{"test": {Run: run}}
}

func TestPoolProcessesManyJobs(t *testing.T) {
	var runs atomic.Int64
	q, p := setup(t, registry(func(context.Context, task.Job) error {
		runs.Add(1)
		return nil
	}), fastConfig())

	for i := 0; i < 50; i++ {
		submit(t, q, "test", 3)
	}
	start(t, p)

	waitFor(t, "50 completed jobs", func() bool { return stats(q).Completed == 50 })
	if s := stats(q); s.Queued != 0 || s.InFlight != 0 {
		t.Fatalf("queue should be empty: %+v", s)
	}
	if runs.Load() != 50 {
		t.Fatalf("expected 50 handler runs, got %d", runs.Load())
	}
}

func TestFailingJobRetriesWithBackoffThenDeadLetters(t *testing.T) {
	var runs atomic.Int64
	q, p := setup(t, registry(func(context.Context, task.Job) error {
		runs.Add(1)
		return errors.New("downstream timeout")
	}), fastConfig())
	start(t, p)

	job := submit(t, q, "test", 3)
	waitFor(t, "job to be dead-lettered", func() bool { return status(q, job.JobID) == task.StatusFailed })

	got, _ := q.GetJob(context.Background(), job.JobID)
	if got.Attempts != 3 || got.ErrorMessage != "downstream timeout" {
		t.Fatalf("unexpected final job: %+v", got)
	}
	if runs.Load() != 3 {
		t.Fatalf("handler should run exactly 3 times, ran %d", runs.Load())
	}
	if s := stats(q); s.Retried != 2 || s.Failed != 1 || s.DeadLetter != 1 {
		t.Fatalf("expected 2 retries and 1 dead letter: %+v", s)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	var runs atomic.Int64
	q, p := setup(t, registry(func(context.Context, task.Job) error {
		runs.Add(1)
		return handlers.Permanent(errors.New("invalid image"))
	}), fastConfig())
	start(t, p)

	job := submit(t, q, "test", 5)
	waitFor(t, "dead letter", func() bool { return status(q, job.JobID) == task.StatusFailed })
	if runs.Load() != 1 || stats(q).Retried != 0 {
		t.Fatalf("permanent error should not retry: runs=%d stats=%+v", runs.Load(), stats(q))
	}
}

func TestHandlerPanicDoesNotKillWorker(t *testing.T) {
	q, p := setup(t, registry(func(_ context.Context, j task.Job) error {
		if j.Payload["boom"] == true {
			panic("nil map write")
		}
		return nil
	}), fastConfig())
	start(t, p)

	bad := task.NewJob("test", map[string]any{"boom": true}, 1)
	_ = q.Enqueue(context.Background(), bad)
	good := submit(t, q, "test", 1)

	waitFor(t, "bad job dead-lettered", func() bool { return status(q, bad.JobID) == task.StatusFailed })
	waitFor(t, "good job completed", func() bool { return status(q, good.JobID) == task.StatusCompleted })
}

func TestUnknownTaskTypeIsDeadLettered(t *testing.T) {
	q, p := setup(t, registry(func(context.Context, task.Job) error { return nil }), fastConfig())
	start(t, p)

	job := submit(t, q, "does_not_exist", 3)
	waitFor(t, "dead letter", func() bool { return status(q, job.JobID) == task.StatusFailed })
}

// End-to-end crash recovery: a worker takes a job and dies without acking.
// A healthy pool must reclaim and finish it.
func TestJobFromCrashedWorkerIsRecovered(t *testing.T) {
	cfg := fastConfig()
	cfg.JobTimeout = 50 * time.Millisecond
	cfg.ClaimIdle = 150 * time.Millisecond
	q, p := setup(t, registry(func(context.Context, task.Job) error { return nil }), cfg)
	if err := q.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}

	job := submit(t, q, "test", 3)
	msgs, err := q.Read(context.Background(), "worker-that-crashed", 100*time.Millisecond)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("setup read failed: %v", err)
	}
	// ...and it never acks.

	start(t, p)
	waitFor(t, "reclaimed job to complete", func() bool { return status(q, job.JobID) == task.StatusCompleted })
	if s := stats(q); s.InFlight != 0 {
		t.Fatalf("nothing should remain in flight: %+v", s)
	}
}

// Graceful shutdown: Stop must wait for a running job and record its result,
// rather than abandoning it.
func TestStopWaitsForInFlightJob(t *testing.T) {
	started := make(chan struct{})
	q, p := setup(t, registry(func(context.Context, task.Job) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		return nil
	}), fastConfig())
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	job := submit(t, q, "test", 1)
	<-started
	p.Stop()

	if got := status(q, job.JobID); got != task.StatusCompleted {
		t.Fatalf("in-flight job should complete during shutdown, got %s", got)
	}
	if s := stats(q); s.InFlight != 0 || s.Workers != 0 {
		t.Fatalf("after stop: %+v", s)
	}
}

package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"WorkQueue/internal/task"
	"WorkQueue/internal/testredis"

	"github.com/redis/go-redis/v9"
)

func newQueue(t *testing.T) *Queue {
	t.Helper()
	rdb, prefix := testredis.Client(t)
	q := New(rdb, prefix)
	if err := q.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	return q
}

func enqueue(t *testing.T, q *Queue, typ string) task.Job {
	t.Helper()
	job := task.NewJob(typ, map[string]any{"n": 1.0}, 3)
	if err := q.Enqueue(context.Background(), job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return job
}

func readOne(t *testing.T, q *Queue, consumer string) Message {
	t.Helper()
	msgs, err := q.Read(context.Background(), consumer, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	return msgs[0]
}

func TestEnsureGroupIsIdempotent(t *testing.T) {
	q := newQueue(t)
	if err := q.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("second EnsureGroup should succeed: %v", err)
	}
}

func TestEnqueueReadComplete(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	job := enqueue(t, q, "generate_pdf")

	got, err := q.GetJob(ctx, job.JobID)
	if err != nil || got.Status != task.StatusPending {
		t.Fatalf("status after enqueue: %v %v", got.Status, err)
	}

	m := readOne(t, q, "c1")
	if m.Job.JobID != job.JobID || m.Job.Payload["n"] != 1.0 {
		t.Fatalf("wrong job read: %+v", m.Job)
	}
	if s, _ := q.Stats(ctx); s.InFlight != 1 || s.Queued != 0 {
		t.Fatalf("expected 1 in flight, 0 queued, got %+v", s)
	}

	m.Job.MarkProcessing()
	m.Job.MarkCompleted()
	if err := q.Complete(ctx, m.ID, m.Job); err != nil {
		t.Fatal(err)
	}

	s, _ := q.Stats(ctx)
	if s.InFlight != 0 || s.Queued != 0 || s.Completed != 1 {
		t.Fatalf("after complete: %+v", s)
	}
	got, _ = q.GetJob(ctx, job.JobID)
	if got.Status != task.StatusCompleted {
		t.Fatalf("expected COMPLETED, got %s", got.Status)
	}
}

func TestReadTimesOutWithNoJobs(t *testing.T) {
	q := newQueue(t)
	msgs, err := q.Read(context.Background(), "c1", 50*time.Millisecond)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("expected empty read, got %d msgs, err %v", len(msgs), err)
	}
}

// A worker reads a job and "crashes" (never acks). Another worker must be able
// to reclaim it. This is the guarantee the old BLPOP design could not give.
func TestCrashedWorkerJobIsReclaimed(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	job := enqueue(t, q, "send_email")

	_ = readOne(t, q, "crashed-worker") // read but never acknowledged

	if more, _ := q.Read(ctx, "healthy-worker", 50*time.Millisecond); len(more) != 0 {
		t.Fatal("an un-acked job must not be delivered to a second worker as new")
	}

	claimed, err := q.ClaimStale(ctx, "healthy-worker", time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatal("job should not be claimable before it has been idle long enough")
	}

	time.Sleep(20 * time.Millisecond)
	claimed, err = q.ClaimStale(ctx, "healthy-worker", 10*time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Job.JobID != job.JobID {
		t.Fatalf("expected to reclaim job %s, got %+v", job.JobID, claimed)
	}
}

func TestRetryWaitsForBackoffThenReturns(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	job := enqueue(t, q, "flaky_task")

	m := readOne(t, q, "c1")
	m.Job.MarkProcessing()
	runAt := time.Now().Add(time.Hour)
	m.Job.MarkRetrying(errors.New("timeout"), runAt)
	if err := q.Retry(ctx, m.ID, m.Job); err != nil {
		t.Fatal(err)
	}

	s, _ := q.Stats(ctx)
	if s.Retrying != 1 || s.InFlight != 0 || s.Queued != 0 || s.Retried != 1 {
		t.Fatalf("after retry: %+v", s)
	}

	if n, _ := q.PromoteDue(ctx, time.Now(), 10); n != 0 {
		t.Fatal("retry must not be promoted before its delay")
	}
	if n, _ := q.PromoteDue(ctx, runAt.Add(time.Millisecond), 10); n != 1 {
		t.Fatalf("expected 1 promoted retry, got %d", n)
	}
	if n, _ := q.PromoteDue(ctx, runAt.Add(time.Millisecond), 10); n != 0 {
		t.Fatal("a retry must only be promoted once")
	}

	again := readOne(t, q, "c1")
	if again.Job.JobID != job.JobID || again.Job.Attempts != 1 {
		t.Fatalf("promoted job wrong: %+v", again.Job)
	}
}

func TestDeadLetterAndReplay(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	job := enqueue(t, q, "flaky_task")

	m := readOne(t, q, "c1")
	m.Job.MarkProcessing()
	m.Job.MarkFailed(errors.New("gave up"))
	if err := q.DeadLetter(ctx, m.ID, m.Job); err != nil {
		t.Fatal(err)
	}

	dls, err := q.DeadLetters(ctx, 10)
	if err != nil || len(dls) != 1 {
		t.Fatalf("expected 1 dead letter, got %d (%v)", len(dls), err)
	}
	if dls[0].Job.ErrorMessage != "gave up" {
		t.Fatalf("error message not kept: %+v", dls[0].Job)
	}
	if s, _ := q.Stats(ctx); s.DeadLetter != 1 || s.Failed != 1 || s.InFlight != 0 {
		t.Fatalf("after dead letter: %+v", s)
	}

	replayed, err := q.Replay(ctx, dls[0].EntryID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Attempts != 0 || replayed.Status != task.StatusPending {
		t.Fatalf("replay should reset the job: %+v", replayed)
	}
	if _, err := q.Replay(ctx, dls[0].EntryID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second replay should be not found, got %v", err)
	}

	back := readOne(t, q, "c1")
	if back.Job.JobID != job.JobID {
		t.Fatal("replayed job should be back on the stream")
	}
}

func TestGetJobNotFound(t *testing.T) {
	q := newQueue(t)
	if _, err := q.GetJob(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMalformedEntryIsDropped(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	q.rdb.XAdd(ctx, &redis.XAddArgs{Stream: q.streamKey(), Values: map[string]any{field: "not json"}})

	msgs, err := q.Read(ctx, "c1", 50*time.Millisecond)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("malformed entry should be skipped, got %d msgs err %v", len(msgs), err)
	}
	if s, _ := q.Stats(ctx); s.InFlight != 0 || s.Queued != 0 {
		t.Fatalf("malformed entry should be acked and removed: %+v", s)
	}
}

func TestHeartbeatCountsWorkers(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()
	_ = q.Heartbeat(ctx, "a", 5, time.Minute)
	_ = q.Heartbeat(ctx, "b", 3, time.Minute)

	if s, _ := q.Stats(ctx); s.Workers != 8 {
		t.Fatalf("expected 8 workers, got %d", s.Workers)
	}
	_ = q.RemoveHeartbeat(ctx, "a")
	if s, _ := q.Stats(ctx); s.Workers != 3 {
		t.Fatalf("expected 3 workers after removal, got %d", s.Workers)
	}
}

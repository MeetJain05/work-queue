// Package queue stores jobs in Redis.
//
// Redis keys (prefix defaults to "wq"):
//
//	wq:jobs          stream   jobs waiting for, or being worked on by, a worker
//	wq:retry         zset     failed jobs waiting for their backoff delay (score = run-at in ms)
//	wq:dlq           stream   dead-letter queue: jobs that ran out of attempts
//	wq:job:<id>      string   latest status of each job (JSON, expires after StatusTTL)
//	wq:stats         hash     completed / retried / failed counters shared by all workers
//	wq:worker:<name> string   heartbeat for each live worker process
//
// Delivery is at-least-once: a job stays in the stream's pending list until a
// worker acknowledges it, so if the worker crashes another worker can reclaim it.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"WorkQueue/internal/task"

	"github.com/redis/go-redis/v9"
)

const (
	group     = "workers"
	field     = "job"
	StatusTTL = 24 * time.Hour
)

// ErrNotFound is returned when a job or dead letter does not exist.
var ErrNotFound = errors.New("not found")

// Message is one stream entry handed to a worker.
type Message struct {
	ID  string // Redis stream entry ID, used to acknowledge the message
	Job task.Job
}

// DeadLetter is one entry in the dead-letter queue.
type DeadLetter struct {
	EntryID string   `json:"entry_id"`
	Job     task.Job `json:"job"`
}

// Stats is a snapshot of the whole system, shared by every producer and worker.
type Stats struct {
	Queued     int64 `json:"queued"`      // in the stream, not yet picked up
	InFlight   int64 `json:"in_flight"`   // delivered to a worker, not yet acknowledged
	Retrying   int64 `json:"retrying"`    // waiting for a backoff delay
	DeadLetter int64 `json:"dead_letter"` // currently in the dead-letter queue
	Completed  int64 `json:"completed"`
	Retried    int64 `json:"retried"`
	Failed     int64 `json:"failed"`
	Workers    int64 `json:"workers"` // worker goroutines across all live worker processes
}

// Queue is a Redis-backed job queue.
type Queue struct {
	rdb    *redis.Client
	prefix string
}

// New returns a queue that stores its keys under prefix (for example "wq").
func New(rdb *redis.Client, prefix string) *Queue {
	if prefix == "" {
		prefix = "wq"
	}
	return &Queue{rdb: rdb, prefix: prefix}
}

func (q *Queue) streamKey() string         { return q.prefix + ":jobs" }
func (q *Queue) retryKey() string          { return q.prefix + ":retry" }
func (q *Queue) dlqKey() string            { return q.prefix + ":dlq" }
func (q *Queue) statsKey() string          { return q.prefix + ":stats" }
func (q *Queue) jobKey(id string) string   { return q.prefix + ":job:" + id }
func (q *Queue) workerKey(n string) string { return q.prefix + ":worker:" + n }

// EnsureGroup creates the stream and consumer group if they do not exist yet.
func (q *Queue) EnsureGroup(ctx context.Context) error {
	err := q.rdb.XGroupCreateMkStream(ctx, q.streamKey(), group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

// Enqueue saves the job's status and appends it to the stream in one transaction.
func (q *Queue) Enqueue(ctx context.Context, job task.Job) error {
	data, err := task.Encode(job)
	if err != nil {
		return err
	}
	_, err = q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, q.jobKey(job.JobID), data, StatusTTL)
		p.XAdd(ctx, &redis.XAddArgs{Stream: q.streamKey(), Values: map[string]any{field: data}})
		return nil
	})
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	return nil
}

// Read blocks for up to block waiting for one new message for this consumer.
// It returns no messages and no error when the wait times out.
func (q *Queue) Read(ctx context.Context, consumer string, block time.Duration) ([]Message, error) {
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{q.streamKey(), ">"},
		Count:    1,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	var msgs []redis.XMessage
	for _, s := range res {
		msgs = append(msgs, s.Messages...)
	}
	return q.decode(ctx, msgs), nil
}

// ClaimStale takes over messages that another worker received but has not
// acknowledged for at least minIdle, which usually means that worker crashed.
func (q *Queue) ClaimStale(ctx context.Context, consumer string, minIdle time.Duration, count int64) ([]Message, error) {
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.streamKey(),
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    count,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("claim stale: %w", err)
	}
	return q.decode(ctx, msgs), nil
}

// decode turns stream entries into jobs. Entries that cannot be decoded are
// acknowledged and removed so they cannot block the queue forever.
func (q *Queue) decode(ctx context.Context, entries []redis.XMessage) []Message {
	out := make([]Message, 0, len(entries))
	for _, e := range entries {
		raw, _ := e.Values[field].(string)
		job, err := task.Decode([]byte(raw))
		if err != nil {
			q.ackAndDelete(ctx, q.rdb, e.ID)
			continue
		}
		out = append(out, Message{ID: e.ID, Job: job})
	}
	return out
}

func (q *Queue) ackAndDelete(ctx context.Context, c redis.Cmdable, id string) {
	c.XAck(ctx, q.streamKey(), group, id)
	c.XDel(ctx, q.streamKey(), id)
}

// SaveStatus stores the job's latest state for the status API.
func (q *Queue) SaveStatus(ctx context.Context, job task.Job) error {
	data, err := task.Encode(job)
	if err != nil {
		return err
	}
	return q.rdb.Set(ctx, q.jobKey(job.JobID), data, StatusTTL).Err()
}

// Complete acknowledges a successful job.
func (q *Queue) Complete(ctx context.Context, msgID string, job task.Job) error {
	return q.finish(ctx, msgID, job, "completed", nil)
}

// Retry acknowledges a failed attempt and schedules the job to run again at
// job.NextRetryAt. The ack and the schedule happen in one transaction, so the
// job is never in both places or in neither.
func (q *Queue) Retry(ctx context.Context, msgID string, job task.Job) error {
	if job.NextRetryAt == nil {
		return errors.New("retry: next_retry_at is not set")
	}
	data, err := task.Encode(job)
	if err != nil {
		return err
	}
	score := float64(job.NextRetryAt.UnixMilli())
	return q.finish(ctx, msgID, job, "retried", func(p redis.Pipeliner) {
		p.ZAdd(ctx, q.retryKey(), redis.Z{Score: score, Member: data})
	})
}

// DeadLetter acknowledges a job that ran out of attempts and moves it to the DLQ.
func (q *Queue) DeadLetter(ctx context.Context, msgID string, job task.Job) error {
	data, err := task.Encode(job)
	if err != nil {
		return err
	}
	return q.finish(ctx, msgID, job, "failed", func(p redis.Pipeliner) {
		p.XAdd(ctx, &redis.XAddArgs{Stream: q.dlqKey(), Values: map[string]any{field: data}})
	})
}

// finish runs ack + status update + counter + any extra command atomically.
func (q *Queue) finish(ctx context.Context, msgID string, job task.Job, counter string, extra func(redis.Pipeliner)) error {
	data, err := task.Encode(job)
	if err != nil {
		return err
	}
	_, err = q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if extra != nil {
			extra(p)
		}
		q.ackAndDelete(ctx, p, msgID)
		p.Set(ctx, q.jobKey(job.JobID), data, StatusTTL)
		p.HIncrBy(ctx, q.statsKey(), counter, 1)
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s: %w", counter, err)
	}
	return nil
}

// promoteScript moves every retry whose time has come back into the stream.
// It runs inside Redis as one atomic step, so two workers can never promote the
// same job and a crash can never leave a job removed from the retry set but
// not yet added to the stream.
var promoteScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, job in ipairs(due) do
  redis.call('ZREM', KEYS[1], job)
  redis.call('XADD', KEYS[2], '*', 'job', job)
end
return #due
`)

// PromoteDue moves up to limit retries whose backoff has expired back onto the stream.
func (q *Queue) PromoteDue(ctx context.Context, now time.Time, limit int) (int, error) {
	n, err := promoteScript.Run(ctx, q.rdb, []string{q.retryKey(), q.streamKey()}, now.UnixMilli(), limit).Int()
	if err != nil {
		return 0, fmt.Errorf("promote retries: %w", err)
	}
	return n, nil
}

// GetJob returns the latest known state of a job.
func (q *Queue) GetJob(ctx context.Context, id string) (task.Job, error) {
	data, err := q.rdb.Get(ctx, q.jobKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return task.Job{}, ErrNotFound
	}
	if err != nil {
		return task.Job{}, err
	}
	return task.Decode(data)
}

// DeadLetters returns the newest entries in the dead-letter queue.
func (q *Queue) DeadLetters(ctx context.Context, limit int64) ([]DeadLetter, error) {
	entries, err := q.rdb.XRevRangeN(ctx, q.dlqKey(), "+", "-", limit).Result()
	if err != nil {
		return nil, err
	}
	out := make([]DeadLetter, 0, len(entries))
	for _, e := range entries {
		raw, _ := e.Values[field].(string)
		if job, err := task.Decode([]byte(raw)); err == nil {
			out = append(out, DeadLetter{EntryID: e.ID, Job: job})
		}
	}
	return out, nil
}

// Replay removes a job from the dead-letter queue and enqueues it again with
// its attempt counter reset. XDEL returns 0 if someone else already replayed
// it, which stops the same dead letter from being replayed twice.
func (q *Queue) Replay(ctx context.Context, entryID string) (task.Job, error) {
	entries, err := q.rdb.XRange(ctx, q.dlqKey(), entryID, entryID).Result()
	if err != nil {
		return task.Job{}, err
	}
	if len(entries) == 0 {
		return task.Job{}, ErrNotFound
	}
	raw, _ := entries[0].Values[field].(string)
	job, err := task.Decode([]byte(raw))
	if err != nil {
		return task.Job{}, err
	}
	if deleted, err := q.rdb.XDel(ctx, q.dlqKey(), entryID).Result(); err != nil {
		return task.Job{}, err
	} else if deleted == 0 {
		return task.Job{}, ErrNotFound
	}

	job.Attempts = 0
	job.ErrorMessage = ""
	job.NextRetryAt = nil
	job.Status = task.StatusPending
	job.UpdatedAt = time.Now().UTC()
	return job, q.Enqueue(ctx, job)
}

// Heartbeat records that a worker process with n goroutines is alive.
func (q *Queue) Heartbeat(ctx context.Context, consumer string, n int, ttl time.Duration) error {
	return q.rdb.Set(ctx, q.workerKey(consumer), n, ttl).Err()
}

// RemoveHeartbeat deletes a worker's heartbeat on clean shutdown.
func (q *Queue) RemoveHeartbeat(ctx context.Context, consumer string) error {
	return q.rdb.Del(ctx, q.workerKey(consumer)).Err()
}

// Stats reads queue depth and counters for the dashboard and /metrics.
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	pipe := q.rdb.Pipeline()
	streamLen := pipe.XLen(ctx, q.streamKey())
	retrying := pipe.ZCard(ctx, q.retryKey())
	dlq := pipe.XLen(ctx, q.dlqKey())
	counters := pipe.HGetAll(ctx, q.statsKey())
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return s, fmt.Errorf("stats: %w", err)
	}

	// XPENDING fails if the group does not exist yet, which just means nothing is in flight.
	if p, err := q.rdb.XPending(ctx, q.streamKey(), group).Result(); err == nil {
		s.InFlight = p.Count
	}

	s.Queued = max(streamLen.Val()-s.InFlight, 0)
	s.Retrying = retrying.Val()
	s.DeadLetter = dlq.Val()
	c := counters.Val()
	s.Completed, _ = strconv.ParseInt(c["completed"], 10, 64)
	s.Retried, _ = strconv.ParseInt(c["retried"], 10, 64)
	s.Failed, _ = strconv.ParseInt(c["failed"], 10, 64)

	var cursor uint64
	for {
		keys, next, err := q.rdb.Scan(ctx, cursor, q.workerKey("*"), 100).Result()
		if err != nil {
			return s, fmt.Errorf("stats workers: %w", err)
		}
		if len(keys) > 0 {
			vals, err := q.rdb.MGet(ctx, keys...).Result()
			if err != nil {
				return s, fmt.Errorf("stats workers: %w", err)
			}
			for _, v := range vals {
				if str, ok := v.(string); ok {
					n, _ := strconv.ParseInt(str, 10, 64)
					s.Workers += n
				}
			}
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	return s, nil
}

// Ping checks the Redis connection.
func (q *Queue) Ping(ctx context.Context) error { return q.rdb.Ping(ctx).Err() }

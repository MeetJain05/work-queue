// Package worker runs a fixed pool of goroutines that pull jobs from the queue.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"WorkQueue/internal/handlers"
	"WorkQueue/internal/queue"
	"WorkQueue/internal/task"
)

// Config controls the pool. Zero values are replaced by defaults.
type Config struct {
	Workers    int           // goroutines processing jobs
	Consumer   string        // this process's name in the consumer group
	JobTimeout time.Duration // max time one attempt may run
	ClaimIdle  time.Duration // reclaim messages un-acked for longer than this (must exceed JobTimeout)
	RetryBase  time.Duration // first retry delay; doubles each attempt
	RetryMax   time.Duration // cap on the retry delay
	Tick       time.Duration // how often to promote due retries
}

func (c *Config) defaults() {
	if c.Workers <= 0 {
		c.Workers = 5
	}
	if c.Consumer == "" {
		host, _ := os.Hostname()
		c.Consumer = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = 30 * time.Second
	}
	if c.ClaimIdle <= c.JobTimeout {
		c.ClaimIdle = 2 * c.JobTimeout
	}
	if c.RetryBase <= 0 {
		c.RetryBase = time.Second
	}
	if c.RetryMax <= 0 {
		c.RetryMax = time.Minute
	}
	if c.Tick <= 0 {
		c.Tick = 500 * time.Millisecond
	}
}

// ConfigFromEnv reads WORKER_COUNT, JOB_TIMEOUT, RETRY_BASE_DELAY and RETRY_MAX_DELAY.
func ConfigFromEnv() Config {
	var c Config
	c.Workers, _ = strconv.Atoi(os.Getenv("WORKER_COUNT"))
	c.JobTimeout, _ = time.ParseDuration(os.Getenv("JOB_TIMEOUT"))
	c.RetryBase, _ = time.ParseDuration(os.Getenv("RETRY_BASE_DELAY"))
	c.RetryMax, _ = time.ParseDuration(os.Getenv("RETRY_MAX_DELAY"))
	return c
}

// Backoff returns the delay before the next attempt: base * 2^(attempt-1),
// capped at max, then "equal jitter" (half fixed, half random) so many jobs
// that failed together do not all retry at the same instant.
func Backoff(attempt int, base, maxDelay time.Duration) time.Duration {
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	d = min(d, maxDelay)
	half := d / 2
	return half + rand.N(half+1)
}

// Pool is a fixed set of worker goroutines plus two background loops:
// one promotes retries whose delay has passed, one reclaims jobs from crashed workers.
type Pool struct {
	q      *queue.Queue
	reg    handlers.Registry
	cfg    Config
	log    *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPool builds a pool. Call Start to begin processing.
func NewPool(q *queue.Queue, reg handlers.Registry, cfg Config, log *slog.Logger) *Pool {
	cfg.defaults()
	if log == nil {
		log = slog.Default()
	}
	return &Pool{q: q, reg: reg, cfg: cfg, log: log.With("consumer", cfg.Consumer)}
}

// Config returns the pool's effective configuration.
func (p *Pool) Config() Config { return p.cfg }

// Start creates the consumer group and launches all goroutines.
func (p *Pool) Start(ctx context.Context) error {
	if err := p.q.EnsureGroup(ctx); err != nil {
		return err
	}
	ctx, p.cancel = context.WithCancel(ctx)

	for i := 0; i < p.cfg.Workers; i++ {
		p.goLoop(func() { p.workLoop(ctx) })
	}
	p.goLoop(func() { p.every(ctx, p.cfg.Tick, p.promoteRetries) })
	p.goLoop(func() { p.every(ctx, p.cfg.ClaimIdle/2, p.reclaim) })
	p.goLoop(func() { p.every(ctx, 5*time.Second, p.heartbeat) })
	p.heartbeat(ctx)
	return nil
}

// Stop stops taking new jobs and waits for in-flight jobs to finish.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = p.q.RemoveHeartbeat(ctx, p.cfg.Consumer)
}

func (p *Pool) goLoop(f func()) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		f()
	}()
}

// workLoop reads one job at a time until ctx is cancelled.
func (p *Pool) workLoop(ctx context.Context) {
	errDelay := 100 * time.Millisecond
	for ctx.Err() == nil {
		// Block for at most 1s so the loop re-checks ctx regularly; this bounds
		// how long Stop waits for idle workers.
		msgs, err := p.q.Read(ctx, p.cfg.Consumer, time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Redis is probably down: back off instead of spinning in a tight loop.
			p.log.Error("read failed", "err", err, "retry_in", errDelay)
			sleep(ctx, errDelay)
			errDelay = min(errDelay*2, 5*time.Second)
			continue
		}
		errDelay = 100 * time.Millisecond
		for _, m := range msgs {
			p.Handle(m)
		}
	}
}

// Handle runs one attempt of a job and records the outcome. It deliberately
// does not use the pool's context: once a job has started, shutdown waits for
// it to finish (bounded by JobTimeout) rather than abandoning it halfway.
//
// If any Redis write here fails, the message is simply left un-acked and
// another worker will reclaim it after ClaimIdle. That is what makes delivery
// at-least-once.
func (p *Pool) Handle(m queue.Message) {
	job := m.Job
	job.MarkProcessing()
	log := p.log.With("job_id", job.JobID, "task_type", job.TaskType, "attempt", job.Attempts)

	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.JobTimeout+5*time.Second)
	defer cancel()
	if err := p.q.SaveStatus(ctx, job); err != nil {
		log.Warn("save status failed", "err", err)
	}

	start := time.Now()
	err := p.run(job)
	log = log.With("duration_ms", time.Since(start).Milliseconds())

	switch {
	case err == nil:
		job.MarkCompleted()
		if err := p.q.Complete(ctx, m.ID, job); err != nil {
			log.Error("ack failed; job will be reclaimed", "err", err)
			return
		}
		log.Info("job completed")

	case job.CanRetry() && !handlers.IsPermanent(err):
		delay := Backoff(job.Attempts, p.cfg.RetryBase, p.cfg.RetryMax)
		job.MarkRetrying(err, time.Now().Add(delay))
		if err := p.q.Retry(ctx, m.ID, job); err != nil {
			log.Error("schedule retry failed; job will be reclaimed", "err", err)
			return
		}
		log.Warn("job failed, retry scheduled", "err", job.ErrorMessage, "retry_in", delay.Round(time.Millisecond))

	default:
		job.MarkFailed(err)
		if err := p.q.DeadLetter(ctx, m.ID, job); err != nil {
			log.Error("dead-letter failed; job will be reclaimed", "err", err)
			return
		}
		log.Error("job moved to dead-letter queue", "err", job.ErrorMessage, "permanent", handlers.IsPermanent(err))
	}
}

// run calls the handler with a timeout and turns a panic into a normal error,
// so one bad job cannot crash the whole worker process.
func (p *Pool) run(job task.Job) (err error) {
	t, ok := p.reg[job.TaskType]
	if !ok {
		return handlers.Permanent(fmt.Errorf("unsupported task_type %q", job.TaskType))
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.JobTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return t.Run(ctx, job)
}

func (p *Pool) promoteRetries(ctx context.Context) {
	n, err := p.q.PromoteDue(ctx, time.Now(), 100)
	if err != nil && ctx.Err() == nil {
		p.log.Error("promote retries failed", "err", err)
	} else if n > 0 {
		p.log.Debug("promoted retries", "count", n)
	}
}

func (p *Pool) reclaim(ctx context.Context) {
	msgs, err := p.q.ClaimStale(ctx, p.cfg.Consumer, p.cfg.ClaimIdle, 10)
	if err != nil {
		if ctx.Err() == nil {
			p.log.Error("reclaim failed", "err", err)
		}
		return
	}
	for _, m := range msgs {
		p.log.Warn("reclaimed job from an unresponsive worker", "job_id", m.Job.JobID)
		p.Handle(m)
	}
}

func (p *Pool) heartbeat(ctx context.Context) {
	if err := p.q.Heartbeat(ctx, p.cfg.Consumer, p.cfg.Workers, 15*time.Second); err != nil && ctx.Err() == nil {
		p.log.Warn("heartbeat failed", "err", err)
	}
}

func (p *Pool) every(ctx context.Context, d time.Duration, f func(context.Context)) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f(ctx)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

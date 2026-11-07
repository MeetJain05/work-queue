// Command bench measures throughput and end-to-end latency of the queue.
//
// For each worker count it starts a real worker pool against Redis, enqueues
// -jobs jobs from -producers concurrent goroutines, waits for all of them to
// complete, and reads each job's status record to compute latency
// (completed_at - created_at). Every key it writes uses a throwaway prefix
// that is deleted afterwards, so it is safe to run against a dev Redis.
//
//	go run ./cmd/bench -jobs 2000 -workers 1,5,20 -work 10ms
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"WorkQueue/internal/handlers"
	"WorkQueue/internal/queue"
	"WorkQueue/internal/task"
	"WorkQueue/internal/worker"

	"github.com/redis/go-redis/v9"
)

type result struct {
	workers       int
	enqueueRate   float64 // jobs/s accepted by the queue
	throughput    float64 // jobs/s completed
	p50, p95, p99 time.Duration
}

func main() {
	jobs := flag.Int("jobs", 2000, "jobs per run")
	workerList := flag.String("workers", "1,5,20", "comma-separated worker counts to test")
	work := flag.Duration("work", 10*time.Millisecond, "simulated work per job")
	producers := flag.Int("producers", 8, "concurrent goroutines enqueueing jobs")
	flag.Parse()

	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		fail(err)
	}
	opt.PoolSize = 64
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		fail(fmt.Errorf("redis unreachable: %w", err))
	}

	fmt.Printf("WorkQueue benchmark: %d jobs per run, %v simulated work per job, %d producers\n\n", *jobs, *work, *producers)
	fmt.Println("| Workers | Enqueue rate | Throughput | p50 latency | p95 latency | p99 latency |")
	fmt.Println("|--------:|-------------:|-----------:|------------:|------------:|------------:|")
	for _, s := range strings.Split(*workerList, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n <= 0 {
			fail(fmt.Errorf("bad worker count %q", s))
		}
		r, err := run(rdb, n, *jobs, *producers, *work)
		if err != nil {
			fail(err)
		}
		fmt.Printf("| %7d | %8.0f /s | %6.0f /s | %11s | %11s | %11s |\n",
			r.workers, r.enqueueRate, r.throughput, ms(r.p50), ms(r.p95), ms(r.p99))
	}
	fmt.Println("\nLatency = time from enqueue until the job is acknowledged as COMPLETED.")
}

func run(rdb *redis.Client, workers, jobs, producers int, work time.Duration) (result, error) {
	ctx := context.Background()
	prefix := "wqbench-" + randHex()
	defer cleanup(rdb, prefix)

	q := queue.New(rdb, prefix)
	reg := handlers.Registry{"bench": {Run: func(ctx context.Context, _ task.Job) error {
		select {
		case <-time.After(work):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool := worker.NewPool(q, reg, worker.Config{Workers: workers, Consumer: "bench"}, quiet)
	if err := pool.Start(ctx); err != nil {
		return result{}, err
	}
	defer pool.Stop()

	// Enqueue from several goroutines, like several API servers would.
	ids := make([]string, jobs)
	var wg sync.WaitGroup
	var enqueueErr error
	var once sync.Once
	start := time.Now()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := p; i < jobs; i += producers {
				job := task.NewJob("bench", nil, 3)
				ids[i] = job.JobID
				if err := q.Enqueue(ctx, job); err != nil {
					once.Do(func() { enqueueErr = err })
					return
				}
			}
		}(p)
	}
	wg.Wait()
	if enqueueErr != nil {
		return result{}, enqueueErr
	}
	enqueueDur := time.Since(start)

	// Wait until every job is completed.
	deadline := time.Now().Add(10 * time.Minute)
	for {
		s, err := q.Stats(ctx)
		if err != nil {
			return result{}, err
		}
		if s.Completed >= int64(jobs) {
			break
		}
		if time.Now().After(deadline) {
			return result{}, fmt.Errorf("timed out: %d/%d completed", s.Completed, jobs)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Read every job's final status in one pipeline and compute latencies.
	pipe := rdb.Pipeline()
	cmds := make([]*redis.StringCmd, jobs)
	for i, id := range ids {
		cmds[i] = pipe.Get(ctx, prefix+":job:"+id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return result{}, err
	}
	lat := make([]time.Duration, 0, jobs)
	var first, last time.Time
	for _, c := range cmds {
		j, err := task.Decode([]byte(c.Val()))
		if err != nil || j.Status != task.StatusCompleted {
			return result{}, fmt.Errorf("job not completed: %+v %v", j, err)
		}
		lat = append(lat, j.UpdatedAt.Sub(j.CreatedAt))
		if first.IsZero() || j.CreatedAt.Before(first) {
			first = j.CreatedAt
		}
		if j.UpdatedAt.After(last) {
			last = j.UpdatedAt
		}
	}
	slices.Sort(lat)

	return result{
		workers:     workers,
		enqueueRate: float64(jobs) / enqueueDur.Seconds(),
		throughput:  float64(jobs) / last.Sub(first).Seconds(),
		p50:         pct(lat, 50),
		p95:         pct(lat, 95),
		p99:         pct(lat, 99),
	}, nil
}

func pct(sorted []time.Duration, p int) time.Duration {
	i := (len(sorted)*p+99)/100 - 1
	return sorted[max(i, 0)]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000) }

func randHex() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func cleanup(rdb *redis.Client, prefix string) {
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, prefix+":*", 500).Iterator()
	for iter.Next(ctx) {
		rdb.Del(ctx, iter.Val())
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "bench:", err)
	os.Exit(1)
}

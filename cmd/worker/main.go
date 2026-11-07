// Command worker runs the worker pool and exposes /metrics and /healthz.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"WorkQueue/internal/handlers"
	"WorkQueue/internal/logger"
	"WorkQueue/internal/queue"
	"WorkQueue/internal/worker"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	_ = godotenv.Load()
	log := logger.New("worker")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opt, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		log.Error("invalid REDIS_URL", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Error("cannot reach redis", "err", err)
		os.Exit(1)
	}

	q := queue.New(rdb, os.Getenv("QUEUE_PREFIX"))
	pool := worker.NewPool(q, handlers.Default(), worker.ConfigFromEnv(), log)
	if err := pool.Start(ctx); err != nil {
		log.Error("start pool", "err", err)
		os.Exit(1)
	}
	cfg := pool.Config()
	log.Info("worker pool started", "workers", cfg.Workers, "consumer", cfg.Consumer,
		"job_timeout", cfg.JobTimeout.String(), "claim_idle", cfg.ClaimIdle.String())

	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		st, err := q.Stats(r.Context())
		if err != nil {
			http.Error(w, `{"error":"queue unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := q.Ping(r.Context()); err != nil {
			http.Error(w, `{"status":"redis unreachable"}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	port := os.Getenv("PORT_WORKER")
	if port == "" {
		port = "8081"
	}
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
		}
	case <-ctx.Done():
		log.Info("shutdown signal received, finishing in-flight jobs")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
	pool.Stop() // waits for running jobs to finish and be acknowledged
	log.Info("worker stopped")
}

// Command producer serves the HTTP API and dashboard and runs the worker pool.
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"WorkQueue/internal/handlers"
	"WorkQueue/internal/logger"
	"WorkQueue/internal/queue"
	"WorkQueue/internal/task"
	"WorkQueue/internal/worker"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

//go:embed frontend/*
var frontendFiles embed.FS

func main() {
	if err := run(); err != nil {
		logger.New("producer").Error("producer failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	_ = godotenv.Load()
	log := logger.New("producer")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opt, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		return fmt.Errorf("invalid REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("cannot reach redis: %w", err)
	}

	static, err := fs.Sub(frontendFiles, "frontend")
	if err != nil {
		return fmt.Errorf("load frontend: %w", err)
	}

	q := queue.New(rdb, os.Getenv("QUEUE_PREFIX"))
	s := &server{
		q:           q,
		reg:         handlers.Default(),
		maxAttempts: task.MaxAttemptsFromEnv(),
		log:         log,
	}

	pool := worker.NewPool(q, handlers.Default(), worker.ConfigFromEnv(), log)
	if err := pool.Start(ctx); err != nil {
		return fmt.Errorf("start worker pool: %w", err)
	}
	defer pool.Stop()
	cfg := pool.Config()
	log.Info("worker pool started", "workers", cfg.Workers, "consumer", cfg.Consumer,
		"job_timeout", cfg.JobTimeout.String(), "claim_idle", cfg.ClaimIdle.String())

	port := producerPort()
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           s.routes(static),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()
	log.Info("producer listening", "port", port)

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("server failed: %w", err)
		}
	case <-ctx.Done():
		log.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		shutdownErr := srv.Shutdown(shutdownCtx)
		cancel()
		if shutdownErr != nil {
			return fmt.Errorf("shutdown error: %w", shutdownErr)
		}
	}
	log.Info("producer stopped")
	return nil
}

func producerPort() string {
	if port := os.Getenv("PORT"); port != "" {
		return port
	}
	if port := os.Getenv("PORT_PRODUCER"); port != "" {
		return port
	}
	return "8080"
}

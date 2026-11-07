// Command producer serves the HTTP API and dashboard. It only enqueues jobs;
// workers run them.
package main

import (
	"context"
	"embed"
	"errors"
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

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

//go:embed frontend/*
var frontendFiles embed.FS

func main() {
	_ = godotenv.Load()
	log := logger.New("producer")

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

	static, err := fs.Sub(frontendFiles, "frontend")
	if err != nil {
		log.Error("load frontend", "err", err)
		os.Exit(1)
	}

	s := &server{
		q:           queue.New(rdb, os.Getenv("QUEUE_PREFIX")),
		reg:         handlers.Default(),
		maxAttempts: task.MaxAttemptsFromEnv(),
		log:         log,
	}

	port := os.Getenv("PORT_PRODUCER")
	if port == "" {
		port = "8080"
	}
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
			log.Error("server failed", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		log.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("shutdown error", "err", err)
		}
	}
	log.Info("producer stopped")
}

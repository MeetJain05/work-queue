package main

import (
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"

	"WorkQueue/internal/handlers"
	"WorkQueue/internal/queue"
	"WorkQueue/internal/task"
)

// server holds the producer's HTTP handlers.
type server struct {
	q           *queue.Queue
	reg         handlers.Registry
	maxAttempts int
	log         *slog.Logger
}

func (s *server) routes(static fs.FS) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("GET /dlq", s.listDeadLetters)
	mux.HandleFunc("POST /dlq/{entryID}/replay", s.replay)
	mux.HandleFunc("GET /healthz", s.health)
	if static != nil {
		mux.Handle("GET /", http.FileServer(http.FS(static)))
	}
	return mux
}

type createJobRequest struct {
	TaskType string         `json:"task_type"`
	Payload  map[string]any `json:"payload"`
}

// createJob validates the request, enqueues the job and returns 202 Accepted
// immediately; the work itself happens later in a worker.
func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Payload == nil {
		req.Payload = map[string]any{}
	}
	if err := s.reg.Validate(req.TaskType, req.Payload); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	job := task.NewJob(req.TaskType, req.Payload, s.maxAttempts)
	if err := s.q.Enqueue(r.Context(), job); err != nil {
		s.log.Error("enqueue failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "queue unavailable, try again")
		return
	}
	s.log.Info("job enqueued", "job_id", job.JobID, "task_type", job.TaskType)

	w.Header().Set("Location", "/jobs/"+job.JobID)
	writeJSON(w, http.StatusAccepted, map[string]any{
		"job_id":     job.JobID,
		"task_type":  job.TaskType,
		"status":     job.Status,
		"status_url": "/jobs/" + job.JobID,
	})
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.q.GetJob(r.Context(), r.PathValue("id"))
	if errors.Is(err, queue.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found (statuses expire after 24h)")
		return
	}
	if err != nil {
		s.log.Error("get job failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "queue unavailable")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.q.Stats(r.Context())
	if err != nil {
		s.log.Error("stats failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "queue unavailable")
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *server) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	limit, err := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	if err != nil || limit <= 0 || limit > 100 {
		limit = 20
	}
	dls, err := s.q.DeadLetters(r.Context(), limit)
	if err != nil {
		s.log.Error("list dead letters failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "queue unavailable")
		return
	}
	writeJSON(w, http.StatusOK, dls)
}

func (s *server) replay(w http.ResponseWriter, r *http.Request) {
	job, err := s.q.Replay(r.Context(), r.PathValue("entryID"))
	if errors.Is(err, queue.ErrNotFound) {
		writeError(w, http.StatusNotFound, "dead letter not found")
		return
	}
	if err != nil {
		s.log.Error("replay failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "queue unavailable")
		return
	}
	s.log.Info("dead letter replayed", "job_id", job.JobID)
	writeJSON(w, http.StatusAccepted, job)
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.q.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

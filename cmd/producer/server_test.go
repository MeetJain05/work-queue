package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"WorkQueue/internal/handlers"
	"WorkQueue/internal/queue"
	"WorkQueue/internal/task"
	"WorkQueue/internal/testredis"
)

func newTestServer(t *testing.T) (*httptest.Server, *queue.Queue) {
	t.Helper()
	rdb, prefix := testredis.Client(t)
	q := queue.New(rdb, prefix)
	if err := q.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := &server{q: q, reg: handlers.Default(), maxAttempts: 3, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ts := httptest.NewServer(s.routes(nil))
	t.Cleanup(ts.Close)
	return ts, q
}

func post(t *testing.T, url, body string) (*http.Response, map[string]any) {
	t.Helper()
	res, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res, out
}

func TestCreateJobValidation(t *testing.T) {
	ts, _ := newTestServer(t)
	cases := []struct {
		name string
		body string
		want int
	}{
		{"valid email", `{"task_type":"send_email","payload":{"to":"a@b.com","subject":"hi"}}`, http.StatusAccepted},
		{"valid pdf without payload", `{"task_type":"generate_pdf"}`, http.StatusAccepted},
		{"missing subject", `{"task_type":"send_email","payload":{"to":"a@b.com"}}`, http.StatusBadRequest},
		{"unknown type", `{"task_type":"mine_bitcoin"}`, http.StatusBadRequest},
		{"empty type", `{}`, http.StatusBadRequest},
		{"bad json", `{not json`, http.StatusBadRequest},
		{"body too large", `{"task_type":"generate_pdf","payload":{"x":"` + strings.Repeat("a", 70<<10) + `"}}`, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, body := post(t, ts.URL+"/jobs", c.body)
			if res.StatusCode != c.want {
				t.Fatalf("status %d, want %d (body %v)", res.StatusCode, c.want, body)
			}
			if c.want == http.StatusBadRequest && body["error"] == "" {
				t.Fatal("errors should be JSON with an error field")
			}
		})
	}
}

func TestCreateThenGetJob(t *testing.T) {
	ts, _ := newTestServer(t)
	res, body := post(t, ts.URL+"/jobs", `{"task_type":"generate_pdf"}`)
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("create failed: %d", res.StatusCode)
	}
	if res.Header.Get("Location") != body["status_url"] {
		t.Fatal("Location header should point at the status URL")
	}

	get, err := http.Get(ts.URL + body["status_url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	defer get.Body.Close()
	var job task.Job
	_ = json.NewDecoder(get.Body).Decode(&job)
	if get.StatusCode != http.StatusOK || job.JobID != body["job_id"] || job.Status != task.StatusPending {
		t.Fatalf("unexpected job: %d %+v", get.StatusCode, job)
	}
}

func TestGetUnknownJobIs404(t *testing.T) {
	ts, _ := newTestServer(t)
	res, err := http.Get(ts.URL + "/jobs/nope")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", res.StatusCode)
	}
}

func TestWrongMethodIs405(t *testing.T) {
	ts, _ := newTestServer(t)
	res, err := http.Get(ts.URL + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", res.StatusCode)
	}
}

func TestStatsAndReplayUnknown(t *testing.T) {
	ts, _ := newTestServer(t)
	post(t, ts.URL+"/jobs", `{"task_type":"generate_pdf"}`)

	res, err := http.Get(ts.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var s queue.Stats
	_ = json.NewDecoder(res.Body).Decode(&s)
	res.Body.Close()
	if s.Queued != 1 {
		t.Fatalf("expected 1 queued job, got %+v", s)
	}

	r, _ := post(t, ts.URL+"/dlq/0-1/replay", "")
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("replaying a missing dead letter should 404, got %d", r.StatusCode)
	}
}

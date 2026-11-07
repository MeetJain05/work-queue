package handlers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"WorkQueue/internal/task"
)

func TestValidate(t *testing.T) {
	reg := Default()
	cases := []struct {
		name    string
		typ     string
		payload map[string]any
		wantErr bool
	}{
		{"valid email", "send_email", map[string]any{"to": "a@b.com", "subject": "hi"}, false},
		{"email missing subject", "send_email", map[string]any{"to": "a@b.com"}, true},
		{"valid resize", "resize_image", map[string]any{"width": 800.0, "height": 600.0}, false},
		{"resize negative width", "resize_image", map[string]any{"width": -1.0, "height": 600.0}, true},
		{"pdf has no payload rules", "generate_pdf", map[string]any{}, false},
		{"unknown type", "mine_bitcoin", map[string]any{}, true},
		{"empty type", "", map[string]any{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := reg.Validate(c.typ, c.payload)
			if (err != nil) != c.wantErr {
				t.Fatalf("Validate(%q) error = %v, wantErr %v", c.typ, err, c.wantErr)
			}
		})
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("bad input")
	if !IsPermanent(Permanent(base)) {
		t.Fatal("wrapped error should be permanent")
	}
	if !IsPermanent(fmt.Errorf("context: %w", Permanent(base))) {
		t.Fatal("permanent should survive further wrapping")
	}
	if IsPermanent(base) {
		t.Fatal("plain error should not be permanent")
	}
	if !errors.Is(Permanent(base), base) {
		t.Fatal("Permanent should unwrap to the original error")
	}
}

func TestHandlerRespectsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	job := task.NewJob("resize_image", map[string]any{"width": 10.0, "height": 10.0}, 1)

	err := Default()["resize_image"].Run(ctx, job)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestFlakyTaskAlwaysFailsAtRateOne(t *testing.T) {
	job := task.NewJob("flaky_task", map[string]any{"fail_rate": 1.0}, 1)
	if err := Default()["flaky_task"].Run(context.Background(), job); err == nil {
		t.Fatal("fail_rate 1 should always fail")
	}
}

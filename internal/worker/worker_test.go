package worker

import (
	"testing"
	"time"
)

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	base, maxDelay := 100*time.Millisecond, 2*time.Second
	cases := []struct {
		attempt  int
		low, top time.Duration // equal jitter: result is in [d/2, d]
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 200 * time.Millisecond},
		{3, 200 * time.Millisecond, 400 * time.Millisecond},
		{10, 1 * time.Second, 2 * time.Second}, // capped
	}
	for _, c := range cases {
		for i := 0; i < 50; i++ {
			d := Backoff(c.attempt, base, maxDelay)
			if d < c.low || d > c.top {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", c.attempt, d, c.low, c.top)
			}
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{JobTimeout: 10 * time.Second, ClaimIdle: 5 * time.Second}
	c.defaults()
	if c.Workers != 5 || c.Consumer == "" {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if c.ClaimIdle <= c.JobTimeout {
		t.Fatalf("ClaimIdle (%v) must exceed JobTimeout (%v) or live jobs get stolen", c.ClaimIdle, c.JobTimeout)
	}
}

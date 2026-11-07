// Package testredis gives integration tests an isolated Redis key prefix.
package testredis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client connects to REDIS_URL (default localhost) and returns the client plus
// a unique key prefix. Every key under that prefix is deleted when the test ends,
// so tests never touch real queue data.
//
// If Redis is unreachable the test is skipped, unless REQUIRE_REDIS=1 (set in
// CI), in which case it fails so integration tests cannot silently stop running.
func Client(t *testing.T) (*redis.Client, string) {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("invalid REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opt)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		if os.Getenv("REQUIRE_REDIS") == "1" {
			t.Fatalf("Redis required but unreachable: %v", err)
		}
		t.Skipf("Redis not available, skipping integration test: %v", err)
	}

	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	prefix := "wqtest-" + hex.EncodeToString(buf)

	t.Cleanup(func() {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, prefix+":*", 100).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
		rdb.Close()
	})
	return rdb, prefix
}

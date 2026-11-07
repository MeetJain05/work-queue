// Package logger builds the structured JSON logger used by both services.
package logger

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON logger that writes to stdout, so Docker or any log
// collector can pick it up. LOG_LEVEL can be debug, info, warn or error.
//
// Job payloads are never logged, because they can contain personal data
// such as email addresses.
func New(service string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}

package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Destination string
	File        string
	Level       string
	Format      string
}

func Setup(cfg Config) error {
	var writer io.Writer
	destination := strings.ToLower(strings.TrimSpace(cfg.Destination))
	switch destination {
	case "file", "both":
		f, err := openFile(cfg.File)
		if err != nil {
			return err
		}
		if destination == "both" {
			writer = io.MultiWriter(os.Stderr, f)
		} else {
			writer = f
		}
	default:
		writer = os.Stderr
	}

	opts := &slog.HandlerOptions{Level: parseLevel(cfg.Level)}
	var handler slog.Handler
	if strings.EqualFold(strings.TrimSpace(cfg.Format), "json") {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}
	slog.SetDefault(slog.New(handler))
	return nil
}

func openFile(path string) (*os.File, error) {
	if path == "" {
		return nil, fmt.Errorf("log destination is file but no log file path is set")
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create log dir %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", path, err)
	}
	return f, nil
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

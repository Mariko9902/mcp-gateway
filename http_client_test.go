package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLoadHTTPRetryConfigFromEnvDefaults(t *testing.T) {
	cfg := loadHTTPRetryConfigFromEnv("MCP_GATEWAY_HTTP_TEST", 7, 123*time.Millisecond, 9*time.Second)
	if cfg.MaxAttempts != 7 {
		t.Fatalf("expected MaxAttempts=7, got %d", cfg.MaxAttempts)
	}
	if cfg.BaseDelay != 123*time.Millisecond {
		t.Fatalf("expected BaseDelay=123ms, got %v", cfg.BaseDelay)
	}
	if cfg.MaxDelay != 9*time.Second {
		t.Fatalf("expected MaxDelay=9s, got %v", cfg.MaxDelay)
	}
}

func TestDoWithHTTPRetryRetriesOn5xx(t *testing.T) {
	cfg := httpRetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	attempts := 0
	v, err := doWithHTTPRetry(cfg, func() (string, int, error) {
		attempts++
		if attempts < 3 {
			return "", 502, errors.New("bad gateway")
		}
		return "ok", 200, nil
	})
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if v != "ok" {
		t.Fatalf("expected ok, got %q", v)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestDoWithHTTPRetryDoesNotRetryOnContextCanceled(t *testing.T) {
	cfg := httpRetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	attempts := 0
	_, err := doWithHTTPRetry(cfg, func() (struct{}, int, error) {
		attempts++
		return struct{}{}, 0, context.Canceled
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

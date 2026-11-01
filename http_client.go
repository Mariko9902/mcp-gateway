package main

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type httpRetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func loadHTTPRetryConfigFromEnv(prefix string, defaultMaxAttempts int, defaultBaseDelay time.Duration, defaultMaxDelay time.Duration) httpRetryConfig {
	maxAttempts := defaultMaxAttempts
	if v := strings.TrimSpace(os.Getenv(prefix + "_RETRY_MAX_ATTEMPTS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxAttempts = n
		}
	}
	baseDelay := defaultBaseDelay
	if v := strings.TrimSpace(os.Getenv(prefix + "_RETRY_BASE_DELAY")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			baseDelay = d
		}
	}
	maxDelay := defaultMaxDelay
	if v := strings.TrimSpace(os.Getenv(prefix + "_RETRY_MAX_DELAY")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			maxDelay = d
		}
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}
	return httpRetryConfig{MaxAttempts: maxAttempts, BaseDelay: baseDelay, MaxDelay: maxDelay}
}

func httpJitterDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// +/- 20% jitter
	span := int64(d / 5)
	if span <= 0 {
		return d
	}
	off := rand.Int63n(span*2+1) - span
	out := int64(d) + off
	if out < 0 {
		out = 0
	}
	return time.Duration(out)
}

func shouldRetryHTTPError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout() || ne.Temporary()
	}
	return true
}

func newHTTPClient(timeout time.Duration) *http.Client {
	if timeout < 0 {
		timeout = 0
	}
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
	}
	return &http.Client{Timeout: timeout, Transport: tr}
}

func doWithHTTPRetry[T any](cfg httpRetryConfig, fn func() (T, int, error)) (T, error) {
	var zero T
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 300 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 5 * time.Second
	}
	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		v, status, err := fn()
		if err == nil {
			return v, nil
		}
		lastErr = err
		retryable := shouldRetryHTTPError(err)
		if status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout {
			retryable = true
		}
		if status >= 500 && status <= 599 {
			retryable = true
		}
		if !retryable || attempt == cfg.MaxAttempts {
			break
		}
		delay := cfg.BaseDelay * time.Duration(1<<(attempt-1))
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
		time.Sleep(httpJitterDelay(delay))
	}
	return zero, lastErr
}

package main

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func NewGateway(rootDir string, logger *log.Logger, idleTimeout time.Duration, routesPath string, statePath string, routes RoutesConfig) *Gateway {
	if logger == nil {
		logger = log.New(os.Stderr, "mcp-gateway: ", log.LstdFlags|log.LUTC)
	}
	if rootDir == "" {
		rootDir = filepath.Join(mustUserHomeDir(), "Coding", "mcp")
	}
	if idleTimeout <= 0 {
		idleTimeout = envDuration("MCP_GATEWAY_IDLE", 86400*time.Second)
	}
	defaultBackendTimeout := envDuration("MCP_GATEWAY_DEFAULT_TIMEOUT", 86400*time.Second)
	if statePath == "" {
		statePath = "./mcp-gateway.state.json"
	}
	g := &Gateway{
		rootDir:               rootDir,
		logger:                logger,
		idleTimeout:           idleTimeout,
		defaultBackendTimeout: defaultBackendTimeout,
		routesPath:            routesPath,
		statePath:             statePath,
		routes:                routes,
		sticky:                map[string]stickyEntry{},
		sessions:              map[string]*Session{},
	}
	return g
}

func envDuration(key string, defaultValue time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultValue
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return defaultValue
}

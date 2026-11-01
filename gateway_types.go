package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"time"
)

type Gateway struct {
	rootDir               string
	logger                *log.Logger
	idleTimeout           time.Duration
	defaultBackendTimeout time.Duration
	routesPath            string
	statePath             string

	routesMu sync.RWMutex
	routes   RoutesConfig

	stateMu sync.Mutex

	stickyMu sync.Mutex
	sticky   map[string]stickyEntry

	mu       sync.Mutex
	sessions map[string]*Session
}

type stickyEntry struct {
	sessionID string
	expireAt  time.Time
}

type sseMsg struct {
	event string
	data  string
}

type rpcIDMapEntry struct {
	gatewayID string
	origID    any
	createdAt time.Time
}

type Session struct {
	id        string
	mcpName   string
	mode      string // local|http
	createdAt time.Time

	// local backend
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	cancel context.CancelFunc

	// http backend
	remoteBaseURL   string
	remoteSessionID string
	httpClient      *http.Client
	httpHeaders     map[string]string

	// json-rpc id rewrite map (to avoid collisions within a shared session)
	rpcIDSeq uint64
	rpcIDMap map[string]rpcIDMapEntry // key: gateway json-rpc id (string)

	mu          sync.Mutex
	subscribers map[chan sseMsg]struct{}
	backlog     []sseMsg
	lastActive  time.Time
	closed      bool
}

type ResolvedBackend struct {
	Type       string
	Command    string
	Args       []string
	Env        map[string]string
	EnvInherit bool
	WorkingDir string
	Timeout    time.Duration
	URL        string
	Headers    map[string]string
}

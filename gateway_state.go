package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type persistedState struct {
	Sessions []persistedSession `json:"sessions"`
}

type persistedSession struct {
	ID              string            `json:"id"`
	MCPName         string            `json:"mcp_name"`
	Mode            string            `json:"mode"`
	RemoteBaseURL   string            `json:"remote_base_url,omitempty"`
	RemoteSessionID string            `json:"remote_session_id,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	LastActive      time.Time         `json:"last_active"`
}

func (g *Gateway) SaveState() error {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()

	// Snapshot sessions under lock, then marshal/write without holding g.mu.
	var sessions []*Session
	g.mu.Lock()
	for _, s := range g.sessions {
		sessions = append(sessions, s)
	}
	g.mu.Unlock()

	st := persistedState{Sessions: make([]persistedSession, 0, len(sessions))}
	for _, s := range sessions {
		s.mu.Lock()
		closed := s.closed
		ps := persistedSession{
			ID:              s.id,
			MCPName:         s.mcpName,
			Mode:            s.mode,
			RemoteBaseURL:   s.remoteBaseURL,
			RemoteSessionID: s.remoteSessionID,
			Headers:         s.httpHeaders,
			CreatedAt:       s.createdAt,
			LastActive:      s.lastActive,
		}
		s.mu.Unlock()
		if closed {
			continue
		}
		st.Sessions = append(st.Sessions, ps)
	}

	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}

	path := g.statePath
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (g *Gateway) LoadState() error {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()

	path := g.statePath
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var st persistedState
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}

	for _, ps := range st.Sessions {
		if ps.ID == "" || ps.MCPName == "" {
			continue
		}
		if ps.Mode != "http" {
			continue
		}
		if ps.RemoteBaseURL == "" || ps.RemoteSessionID == "" {
			continue
		}

		sess := &Session{
			id:              ps.ID,
			mcpName:         ps.MCPName,
			mode:            "http",
			createdAt:       ps.CreatedAt,
			remoteBaseURL:   ps.RemoteBaseURL,
			remoteSessionID: ps.RemoteSessionID,
			httpClient:      &http.Client{Timeout: 30 * time.Second},
			httpHeaders:     ps.Headers,
			rpcIDMap:        map[string]rpcIDMapEntry{},
			subscribers:     map[chan sseMsg]struct{}{},
			lastActive:      ps.LastActive,
		}

		g.mu.Lock()
		g.sessions[g.sessionKey(ps.MCPName, ps.ID)] = sess
		g.mu.Unlock()

		runCtx, cancel := context.WithCancel(context.Background())
		sess.cancel = cancel
		go g.proxyRemoteEvents(runCtx, sess)
		go g.idleReaper(runCtx, sess)
	}

	return nil
}

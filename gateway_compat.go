package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// Windsurf compatibility mode:
// - Windsurf may init by opening an SSE connection with GET {baseURL}

func (g *Gateway) clientStickyKey(mcpName string, r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return mcpName + "|" + host + "|" + r.UserAgent()
}

func (g *Gateway) stickyGet(key string) (string, bool) {
	if strings.TrimSpace(key) == "" {
		return "", false
	}
	now := time.Now()
	g.stickyMu.Lock()
	defer g.stickyMu.Unlock()
	if g.sticky == nil {
		g.sticky = map[string]stickyEntry{}
		return "", false
	}
	e, ok := g.sticky[key]
	if !ok {
		return "", false
	}
	if now.After(e.expireAt) {
		delete(g.sticky, key)
		return "", false
	}
	return e.sessionID, true
}

func (g *Gateway) stickyPut(key string, sessionID string, ttl time.Duration) {
	if strings.TrimSpace(key) == "" || strings.TrimSpace(sessionID) == "" {
		return
	}
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	g.stickyMu.Lock()
	defer g.stickyMu.Unlock()
	if g.sticky == nil {
		g.sticky = map[string]stickyEntry{}
	}
	g.sticky[key] = stickyEntry{sessionID: sessionID, expireAt: time.Now().Add(ttl)}
}

func (g *Gateway) getSessionFromID(mcpName, sid string) (*Session, bool) {
	s, err := g.getSession(mcpName, sid)
	if err != nil {
		return nil, false
	}
	return s, true
}

func (g *Gateway) cookieNameForMCP(mcpName string) string {
	// cookie name must be token-like; keep stable.
	name := strings.ReplaceAll(mcpName, "/", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return "mcp_session_" + name
}

func (g *Gateway) getSessionIDFromCookie(mcpName string, r *http.Request) string {
	c, err := r.Cookie(g.cookieNameForMCP(mcpName))
	if err != nil {
		return ""
	}
	return c.Value
}

func (g *Gateway) setSessionCookie(mcpName string, w http.ResponseWriter, sid string) {
	if strings.TrimSpace(sid) == "" {
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     g.cookieNameForMCP(mcpName),
		Value:    sid,
		Path:     "/",
		HttpOnly: false,
		SameSite: http.SameSiteLaxMode,
		// no MaxAge/Expires: session cookie
	})
}

// - and later POST JSON-RPC to the same baseURL.
// We bridge this by auto-creating a session on GET /mcps/{name} and keeping
// SSE streaming open (proxying from the session). The session id is returned
// via header and an initial SSE event.

func (g *Gateway) handleCompatEvents(w http.ResponseWriter, r *http.Request, mcpName string) {
	var (
		sess       *Session
		createdNew bool
		usedSticky bool
	)

	// Detect whether client explicitly selected a session.
	explicitSID := strings.TrimSpace(r.Header.Get("X-MCP-Session-ID"))
	if explicitSID == "" {
		explicitSID = strings.TrimSpace(r.URL.Query().Get("session_id"))
	}
	if explicitSID == "" {
		explicitSID = strings.TrimSpace(g.getSessionIDFromCookie(mcpName, r))
	}

	// If client did not explicitly provide a session id/cookie, use a short sticky window
	// to dedupe duplicate GET connections (some clients open multiple SSE connections).
	if explicitSID == "" {
		if sid, ok := g.stickyGet(g.clientStickyKey(mcpName, r)); ok {
			if s2, ok2 := g.getSessionFromID(mcpName, sid); ok2 {
				sess = s2
				usedSticky = true
			}
		}
	}

	// If client provided a session id, try to reuse it.
	if sess == nil {
		if s, ok := g.getSessionFromRequest(mcpName, r); ok {
			sess = s
		}
	}
	if sess != nil && sess.mode == "http" {
		sess.mu.Lock()
		sess.httpHeaders = mergeHeaders(sess.httpHeaders, extractForwardHeaders(r))
		sess.mu.Unlock()
	}
	if sess == nil {
		// If client provided a session id that doesn't exist, create with that id.
		sid := strings.TrimSpace(r.Header.Get("X-MCP-Session-ID"))
		if sid == "" {
			sid = strings.TrimSpace(r.URL.Query().Get("session_id"))
		}
		if sid == "" {
			sid = strings.TrimSpace(g.getSessionIDFromCookie(mcpName, r))
		}
		if sid != "" {
			// Re-check under lock in case concurrent GET created it.
			if s2, ok2 := g.getSessionFromID(mcpName, sid); ok2 {
				sess = s2
			} else {
				backend, err := g.resolveBackend(mcpName)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				clientHeaders := extractForwardHeaders(r)
				s, err := g.newSessionWithIDAndHeaders(r.Context(), mcpName, sid, backend, clientHeaders)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				sess = s
				createdNew = true
			}
		} else {
			backend, err := g.resolveBackend(mcpName)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			sid, err := newSessionID()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			s, err := g.newSessionWithIDAndHeaders(r.Context(), mcpName, sid, backend, extractForwardHeaders(r))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			sess = s
			createdNew = true
		}
	}

	if createdNew {
		g.setSessionCookie(mcpName, w, sess.id)
	} else {
		// Refresh cookie for stickiness.
		g.setSessionCookie(mcpName, w, sess.id)
	}
	if explicitSID == "" {
		g.stickyPut(g.clientStickyKey(mcpName, r), sess.id, 10*time.Second)
	}

	g.logger.Printf("compat-sse mcp=%s remote=%s ua=%q session=%s new=%v sticky=%v", mcpName, r.RemoteAddr, r.UserAgent(), sess.id, createdNew, usedSticky)

	w.Header().Set("X-MCP-Session-ID", sess.id)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := make(chan sseMsg, 128)
	sess.addSubscriber(ch)
	defer sess.removeSubscriber(ch)

	writeEvent := func(event string, data string) error {
		data = strings.ReplaceAll(data, "\n", "\\n")
		_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		if err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	endpoint := fmt.Sprintf("%s://%s/mcps/%s?session_id=%s", scheme, r.Host, mcpName, sess.id)
	_ = writeEvent("endpoint", endpoint)

	// Let client know session id early.
	_ = writeEvent("session", mustJSON(map[string]any{"session_id": sess.id}))
	_ = writeEvent("ready", "1")
	_ = writeEvent("keepalive", "1")

	if createdNew {
		_ = g.SaveState()
		// Immediately send a tools/list to provoke a response (helps detect silent MCPs)
		probe := `{"jsonrpc":"2.0","id":"gw-probe","method":"tools/list","params":{}}`
		if rewritten, _, ok, err := sess.rewriteJSONRPCIDIfNeeded(probe); err == nil {
			if ok {
				probe = rewritten
			}
		}
		_ = sess.writeLine(probe)
	}

	keepalive := time.NewTicker(5 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			// Do NOT close session on disconnect; keep it for reuse.
			return
		case <-keepalive.C:
			if err := writeEvent("keepalive", "1"); err != nil {
				return
			}
		case msg, ok := <-ch:
			if !ok {
				return
			}
			event := msg.event
			if event == "" {
				event = "message"
			}
			// MCP SSE transport expects JSON-RPC responses/notifications to be delivered
			// as event: message. Some clients (e.g. Windsurf) ignore custom event names.
			if event != "keepalive" && event != "ready" && event != "session" && event != "endpoint" {
				event = "message"
			}
			data := sess.restoreJSONRPCIDIfPossible(msg.data)
			if err := writeEvent(event, data); err != nil {
				return
			}
		}
	}
}

func (g *Gateway) getSessionFromRequest(mcpName string, r *http.Request) (*Session, bool) {
	sid := strings.TrimSpace(r.Header.Get("X-MCP-Session-ID"))
	if sid == "" {
		sid = strings.TrimSpace(r.URL.Query().Get("session_id"))
	}
	if sid == "" {
		sid = strings.TrimSpace(g.getSessionIDFromCookie(mcpName, r))
	}
	if sid == "" {
		return nil, false
	}
	s, err := g.getSession(mcpName, sid)
	if err != nil {
		return nil, false
	}
	return s, true
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

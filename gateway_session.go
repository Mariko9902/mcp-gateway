package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

func (g *Gateway) handleCreateSession(w http.ResponseWriter, r *http.Request, mcpName string) {
	sess, err := g.newSession(r.Context(), mcpName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"session_id":%q}`, sess.id)))
}

func (g *Gateway) handleDeleteSession(w http.ResponseWriter, r *http.Request, mcpName, sessionID string) {
	sess, err := g.getSession(mcpName, sessionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	sess.close()
	g.deleteSession(mcpName, sessionID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (g *Gateway) handleEvents(w http.ResponseWriter, r *http.Request, mcpName, sessionID string) {
	sess, err := g.getSession(mcpName, sessionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

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

	_ = writeEvent("ready", "1")
	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if err := writeEvent("keepalive", "1"); err != nil {
				return
			}
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if msg.event == "" {
				msg.event = "message"
			}
			data := sess.restoreJSONRPCIDIfPossible(msg.data)
			if err := writeEvent(msg.event, data); err != nil {
				return
			}
		}
	}
}

func (g *Gateway) handleRPC(w http.ResponseWriter, r *http.Request, mcpName, sessionID string) {
	sess, err := g.getSession(mcpName, sessionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	line := strings.TrimSpace(string(body))
	if line == "" {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	if rewritten, _, ok, err := sess.rewriteJSONRPCIDIfNeeded(line); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	} else if ok {
		line = rewritten
	}

	if err := sess.writeLine(line); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (g *Gateway) newSession(ctx context.Context, mcpName string) (*Session, error) {
	backend, err := g.resolveBackend(mcpName)
	if err != nil {
		return nil, err
	}
	id, err := newSessionID()
	if err != nil {
		return nil, err
	}
	return g.newSessionWithIDAndHeaders(ctx, mcpName, id, backend, nil)
}

func (g *Gateway) newSessionWithID(ctx context.Context, mcpName string, sessionID string, backend ResolvedBackend) (*Session, error) {
	return g.newSessionWithIDAndHeaders(ctx, mcpName, sessionID, backend, nil)
}

func (g *Gateway) newSessionWithIDAndHeaders(ctx context.Context, mcpName string, sessionID string, backend ResolvedBackend, clientHeaders map[string]string) (*Session, error) {
	if backend.Type == "http" {
		return g.newHTTPSession(ctx, mcpName, sessionID, backend, clientHeaders)
	}
	return g.newLocalSession(ctx, mcpName, sessionID, backend)
}

func (g *Gateway) newLocalSession(ctx context.Context, mcpName, sessionID string, backend ResolvedBackend) (*Session, error) {
	// IMPORTANT: do NOT bind the process lifetime to the HTTP request context.
	// When SSE client disconnects, r.Context() is canceled and CommandContext would
	// kill the child process. We want the session to outlive the connection and be
	// reclaimed by idle-timeout or explicit DELETE.
	runCtx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(runCtx, backend.Command, backend.Args...)
	cmd.Env = buildEnv(backend.Env, backend.EnvInherit)
	if backend.WorkingDir != "" {
		cmd.Dir = backend.WorkingDir
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, err
	}

	sess := &Session{
		id:          sessionID,
		mcpName:     mcpName,
		mode:        "local",
		createdAt:   time.Now(),
		cmd:         cmd,
		stdin:       stdin,
		cancel:      cancel,
		rpcIDMap:    map[string]rpcIDMapEntry{},
		subscribers: map[chan sseMsg]struct{}{},
		lastActive:  time.Now(),
	}

	g.mu.Lock()
	g.sessions[g.sessionKey(mcpName, sessionID)] = sess
	g.mu.Unlock()

	if err := cmd.Start(); err != nil {
		g.deleteSession(mcpName, sessionID)
		cancel()
		return nil, err
	}

	go g.readLoop(runCtx, sess, stdout)
	go g.readStderrLoop(runCtx, sess, stderr)
	go g.waitLoop(sess)
	go g.idleReaper(runCtx, sess)
	_ = g.SaveState()

	return sess, nil
}

func (g *Gateway) newHTTPSession(ctx context.Context, mcpName, sessionID string, backend ResolvedBackend, clientHeaders map[string]string) (*Session, error) {
	baseURL := strings.TrimSpace(backend.URL)
	if baseURL == "" {
		return nil, errors.New("http backend url is empty")
	}
	headers := mergeHeaders(backend.Headers, clientHeaders)

	timeout := backend.Timeout
	if timeout <= 0 {
		timeout = g.defaultBackendTimeout
	}
	client := newHTTPClient(timeout)
	retryCfg := loadHTTPRetryConfigFromEnv("MCP_GATEWAY_HTTP", 8, 400*time.Millisecond, 8*time.Second)

	createURL := strings.TrimRight(baseURL, "/") + "/session"
	type createResult struct {
		sid int
		v   struct {
			SessionID string `json:"session_id"`
		}
	}

	res, err := doWithHTTPRetry(retryCfg, func() (createResult, int, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, createURL, nil)
		if err != nil {
			return createResult{}, 0, err
		}
		applyHeaders(req, headers)
		resp, err := client.Do(req)
		if err != nil {
			return createResult{}, 0, fmt.Errorf("create remote session: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			return createResult{}, resp.StatusCode, fmt.Errorf("create remote session: %s: %s", resp.Status, string(b))
		}
		var createResp struct {
			SessionID string `json:"session_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&createResp); err != nil {
			return createResult{}, 0, fmt.Errorf("decode create session response: %w", err)
		}
		out := createResult{}
		out.v.SessionID = createResp.SessionID
		return out, resp.StatusCode, nil
	})
	if err != nil {
		return nil, err
	}
	createResp := res.v
	if createResp.SessionID == "" {
		return nil, errors.New("remote gateway returned empty session_id")
	}

	runCtx, cancel := context.WithCancel(context.Background())
	sess := &Session{
		id:              sessionID,
		mcpName:         mcpName,
		mode:            "http",
		createdAt:       time.Now(),
		remoteBaseURL:   strings.TrimRight(baseURL, "/"),
		remoteSessionID: createResp.SessionID,
		httpClient:      client,
		httpHeaders:     headers,
		cancel:          cancel,
		rpcIDMap:        map[string]rpcIDMapEntry{},
		subscribers:     map[chan sseMsg]struct{}{},
		lastActive:      time.Now(),
	}

	g.mu.Lock()
	g.sessions[g.sessionKey(mcpName, sessionID)] = sess
	g.mu.Unlock()

	go g.proxyRemoteEvents(runCtx, sess)
	go g.idleReaper(runCtx, sess)
	_ = g.SaveState()

	return sess, nil
}

func (g *Gateway) proxyRemoteEvents(ctx context.Context, sess *Session) {
	retryCfg := loadHTTPRetryConfigFromEnv("MCP_GATEWAY_HTTP_EVENTS", 30, 400*time.Millisecond, 10*time.Second)
	client := newHTTPClient(0)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		url := sess.remoteBaseURL + "/session/" + sess.remoteSessionID + "/events"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			g.logger.Printf("[%s][%s] remote events request build error: %v", sess.mcpName, sess.id, err)
			sess.close()
			return
		}
		applyHeaders(req, sess.httpHeaders)

		resp, err := client.Do(req)
		if err != nil {
			g.logger.Printf("[%s][%s] remote events connect error: %v", sess.mcpName, sess.id, err)
			_, _ = doWithHTTPRetry(retryCfg, func() (struct{}, int, error) {
				select {
				case <-ctx.Done():
					return struct{}{}, 0, context.Canceled
				default:
				}
				tmpReq, _ := http.NewRequestWithContext(ctx, http.MethodHead, sess.remoteBaseURL, nil)
				_, _ = tmpReq, err
				return struct{}{}, 0, fmt.Errorf("retry remote events: %w", err)
			})
			continue
		}

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			g.logger.Printf("[%s][%s] remote events non-200: %s: %s", sess.mcpName, sess.id, resp.Status, string(b))
			_, _ = doWithHTTPRetry(retryCfg, func() (struct{}, int, error) {
				return struct{}{}, resp.StatusCode, fmt.Errorf("remote events non-200: %s", resp.Status)
			})
			continue
		}

		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 1<<20), 16<<20)

		var event string
		for sc.Scan() {
			select {
			case <-ctx.Done():
				_ = resp.Body.Close()
				return
			default:
			}
			line := sc.Text()
			if strings.HasPrefix(line, "event: ") {
				event = strings.TrimPrefix(line, "event: ")
				continue
			}
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				if event != "keepalive" && event != "ready" {
					sess.broadcastRaw(sseMsg{event: event, data: data})
				}
				event = ""
			}
		}
		if err := sc.Err(); err != nil {
			g.logger.Printf("[%s][%s] remote events scan error: %v", sess.mcpName, sess.id, err)
		}
		_ = resp.Body.Close()
		// reconnect unless ctx canceled
		continue
	}
}

func (s *Session) writeLine(line string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("session closed")
	}
	s.lastActive = time.Now()
	s.mu.Unlock()

	if s.mode == "http" {
		url := s.remoteBaseURL + "/session/" + s.remoteSessionID + "/rpc"
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(line))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		applyHeaders(req, s.httpHeaders)
		resp, err := s.httpClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}

	_, err := io.WriteString(s.stdin, line+"\n")
	return err
}

func (s *Session) rewriteJSONRPCIDIfNeeded(line string) (rewritten string, gatewayID string, ok bool, err error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return line, "", false, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return line, "", false, nil
	}
	orig, has := m["id"]
	if !has || orig == nil {
		return line, "", false, nil
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return line, "", false, errors.New("session closed")
	}
	if s.rpcIDMap == nil {
		s.rpcIDMap = map[string]rpcIDMapEntry{}
	}
	s.rpcIDSeq++
	gwID := fmt.Sprintf("gw:%s:%d", s.id, s.rpcIDSeq)
	// Record mapping for later restoration.
	s.rpcIDMap[gwID] = rpcIDMapEntry{gatewayID: gwID, origID: orig, createdAt: time.Now()}
	// Best-effort cleanup to avoid unbounded growth.
	const maxEntries = 10000
	expireBefore := time.Now().Add(-30 * time.Minute)
	if len(s.rpcIDMap) > maxEntries {
		for k, v := range s.rpcIDMap {
			if v.createdAt.Before(expireBefore) {
				delete(s.rpcIDMap, k)
			}
		}
		// If still too large, drop oldest-ish by additional sweeping.
		if len(s.rpcIDMap) > maxEntries {
			for k := range s.rpcIDMap {
				delete(s.rpcIDMap, k)
				if len(s.rpcIDMap) <= maxEntries {
					break
				}
			}
		}
	} else {
		for k, v := range s.rpcIDMap {
			if v.createdAt.Before(expireBefore) {
				delete(s.rpcIDMap, k)
			}
		}
	}
	s.mu.Unlock()

	m["id"] = gwID
	b, err := json.Marshal(m)
	if err != nil {
		return line, "", false, nil
	}
	return strings.TrimSpace(string(b)), gwID, true, nil
}

func (s *Session) restoreJSONRPCIDIfPossible(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return line
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return line
	}
	v, ok := m["id"]
	if !ok || v == nil {
		return line
	}
	gwID, ok := v.(string)
	if !ok || strings.TrimSpace(gwID) == "" {
		return line
	}

	s.mu.Lock()
	entry, ok := s.rpcIDMap[gwID]
	s.mu.Unlock()
	if !ok {
		return line
	}
	if entry.origID == nil {
		return line
	}
	m["id"] = entry.origID
	b, err := json.Marshal(m)
	if err != nil {
		return line
	}
	return strings.TrimSpace(string(b))
}

func (s *Session) isResponseMsg(msg sseMsg) bool {
	if msg.event == "response" {
		return true
	}
	if msg.event == "message" {
		return classifyJSONRPCEvent(msg.data) == "response"
	}
	return false
}

func (s *Session) addSubscriber(ch chan sseMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		close(ch)
		return
	}
	s.subscribers[ch] = struct{}{}
	// Replay recent backlog to late subscribers (e.g., clients that POST initialize
	// before establishing SSE). Best-effort: do not block.
	kept := make([]sseMsg, 0, len(s.backlog))
	for _, msg := range s.backlog {
		select {
		case ch <- msg:
		default:
		}
		if !s.isResponseMsg(msg) {
			kept = append(kept, msg)
		}
	}
	s.backlog = kept
}

func (s *Session) removeSubscriber(ch chan sseMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Do NOT close the channel here. Channel lifecycle is managed by Session.close().
	// This avoids double-close when the client disconnect triggers Session.close()
	// while a handler still defers removeSubscriber.
	delete(s.subscribers, ch)
}

func (s *Session) broadcast(raw string) {
	s.broadcastRaw(sseMsg{event: classifyJSONRPCEvent(raw), data: raw})
}

func (s *Session) broadcastRaw(msg sseMsg) {
	s.mu.Lock()
	s.lastActive = time.Now()
	// Keep a small replay backlog.
	const maxBacklog = 256
	if maxBacklog > 0 {
		shouldBacklog := true
		if s.isResponseMsg(msg) {
			shouldBacklog = len(s.subscribers) == 0
			if !shouldBacklog {
				if s.mcpName != "" {
					log.Printf("[%s][%s] skip backlog for json-rpc response (subscribers=%d)", s.mcpName, s.id, len(s.subscribers))
				} else {
					log.Printf("[%s] skip backlog for json-rpc response (subscribers=%d)", s.id, len(s.subscribers))
				}
			}
		}
		if shouldBacklog {
			s.backlog = append(s.backlog, msg)
			if len(s.backlog) > maxBacklog {
				drop := len(s.backlog) - maxBacklog
				s.backlog = append([]sseMsg(nil), s.backlog[drop:]...)
			}
		}
	}
	for ch := range s.subscribers {
		select {
		case ch <- msg:
		default:
		}
	}
	s.mu.Unlock()
}

func (s *Session) isIdle(timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return true
	}
	return time.Since(s.lastActive) > timeout
}

func (s *Session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	subs := make([]chan sseMsg, 0, len(s.subscribers))
	for ch := range s.subscribers {
		subs = append(subs, ch)
		delete(s.subscribers, ch)
	}
	s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
	}

	if s.mode == "local" {
		_ = s.stdin.Close()
		if s.cmd != nil && s.cmd.Process != nil {
			_ = s.cmd.Process.Kill()
		}
	} else if s.mode == "http" {
		// attempt to delete remote session
		if s.remoteBaseURL != "" && s.remoteSessionID != "" {
			url := s.remoteBaseURL + "/session/" + s.remoteSessionID
			req, _ := http.NewRequest(http.MethodDelete, url, nil)
			if req != nil && s.httpClient != nil {
				applyHeaders(req, s.httpHeaders)
				resp, err := s.httpClient.Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}
		}
	}

	for _, ch := range subs {
		close(ch)
	}
}

func (g *Gateway) readLoop(ctx context.Context, sess *Session, stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 1<<20), 16<<20)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		sess.broadcast(line)
	}

	sess.close()
}

func (g *Gateway) readStderrLoop(ctx context.Context, sess *Session, stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		g.logger.Printf("[%s][%s] %s", sess.mcpName, sess.id, line)
	}
}

func (g *Gateway) waitLoop(sess *Session) {
	if sess.cmd == nil {
		return
	}
	err := sess.cmd.Wait()
	if err != nil {
		g.logger.Printf("[%s][%s] process exited with error: %v", sess.mcpName, sess.id, err)
	}
	sess.close()
	g.deleteSession(sess.mcpName, sess.id)
}

func (g *Gateway) idleReaper(ctx context.Context, sess *Session) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if sess.isIdle(g.idleTimeout) {
				sess.close()
				g.deleteSession(sess.mcpName, sess.id)
				return
			}
		}
	}
}

func (g *Gateway) getSession(mcpName, sessionID string) (*Session, error) {
	g.mu.Lock()
	sess := g.sessions[g.sessionKey(mcpName, sessionID)]
	g.mu.Unlock()
	if sess == nil {
		return nil, errors.New("session not found")
	}
	return sess, nil
}

func (g *Gateway) deleteSession(mcpName, sessionID string) {
	g.mu.Lock()
	delete(g.sessions, g.sessionKey(mcpName, sessionID))
	g.mu.Unlock()
	_ = g.SaveState()
}

func (g *Gateway) sessionKey(mcpName, sessionID string) string {
	return mcpName + ":" + sessionID
}

func init() {
	// ensure os package referenced (lint)
	_ = os.ErrClosed
}

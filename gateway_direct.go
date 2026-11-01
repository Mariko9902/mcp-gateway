package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func parseJSONRPCID(line string) (string, bool) {
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return "", false
	}
	v, ok := m["id"]
	if !ok || v == nil {
		return "", false
	}
	switch x := v.(type) {
	case string:
		if strings.TrimSpace(x) == "" {
			return "", false
		}
		return "s:" + x, true
	case float64:
		// json numbers decode as float64
		return "n:" + strconv.FormatFloat(x, 'f', -1, 64), true
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return "", false
		}
		return "j:" + string(b), true
	}
}

func parseJSONRPCMethod(line string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return ""
	}
	if v, ok := m["method"]; ok {
		if s, ok2 := v.(string); ok2 {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func (g *Gateway) handleDirectRPC(w http.ResponseWriter, r *http.Request, mcpName string) {
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
	line = injectDisabledToolsIfNeeded(line, r)

	method := parseJSONRPCMethod(line)

	// If client explicitly picks a session (session_id/cookie/header), assume it is
	// using MCP SSE transport endpoint. In this mode, POST should not synchronously
	// return JSON-RPC result; instead, responses are delivered via SSE events.
	explicitSID := strings.TrimSpace(r.Header.Get("X-MCP-Session-ID"))
	if explicitSID == "" {
		explicitSID = strings.TrimSpace(r.URL.Query().Get("session_id"))
	}
	if explicitSID == "" {
		explicitSID = strings.TrimSpace(g.getSessionIDFromCookie(mcpName, r))
	}

	// Windsurf compat:
	// - Some clients POST first (without session_id/cookie), then GET SSE later.
	// - We must ensure both calls use the same long-lived session/process.
	// So if no explicit session is provided, we reuse/create one using a client-sticky key.
	var (
		sess       *Session
		createdNew bool
	)
	if s, ok := g.getSessionFromRequest(mcpName, r); ok {
		sess = s
	}
	if sess == nil {
		if sid, ok := g.stickyGet(g.clientStickyKey(mcpName, r)); ok {
			if s2, ok2 := g.getSessionFromID(mcpName, sid); ok2 {
				sess = s2
			}
		}
	}
	if sess == nil {
		backend, err := g.resolveBackend(mcpName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		sid, err := newSessionID()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s, err := g.newSessionWithIDAndHeaders(r.Context(), mcpName, sid, backend, extractForwardHeaders(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		sess = s
		createdNew = true
	}
	if sess != nil {
		// Make sure the follow-up GET can reuse this session.
		g.setSessionCookie(mcpName, w, sess.id)
		g.stickyPut(g.clientStickyKey(mcpName, r), sess.id, 2*time.Minute)
		if createdNew {
			_ = g.SaveState()
		}
	}

	if sess != nil {
		rewritten, _, ok, err := sess.rewriteJSONRPCIDIfNeeded(line)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if ok {
			line = rewritten
		}

		idKey, hasID := parseJSONRPCID(line)
		if hasID {
			g.logger.Printf("direct-rpc mcp=%s method=%q id=%s session=%s mode=%s", mcpName, method, idKey, sess.id, sess.mode)
		} else {
			g.logger.Printf("direct-notify mcp=%s method=%q session=%s mode=%s", mcpName, method, sess.id, sess.mode)
		}

		if explicitSID != "" {
			// SSE transport mode: enqueue request/notification and return immediately.
			if err := sess.writeLine(line); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		if !hasID {
			// Non-SSE mode but still a JSON-RPC notification: forward and ack.
			if err := sess.writeLine(line); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}

		// If it's an HTTP backend session, update headers from this request (token refresh).
		if sess.mode == "http" {
			sess.mu.Lock()
			sess.httpHeaders = mergeHeaders(sess.httpHeaders, extractForwardHeaders(r))
			headers := sess.httpHeaders
			client := sess.httpClient
			base := sess.remoteBaseURL
			rsid := sess.remoteSessionID
			sess.mu.Unlock()

			url := base + "/session/" + rsid + "/rpc"
			req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, url, strings.NewReader(line))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			applyHeaders(req, headers)
			resp, err := client.Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			restored := sess.restoreJSONRPCIDIfPossible(strings.TrimSpace(string(b)))
			_, _ = w.Write([]byte(restored))
			return
		}

		// Local session: wait for matching response.
		ch := make(chan sseMsg, 128)
		sess.addSubscriber(ch)
		defer sess.removeSubscriber(ch)
		// Important: addSubscriber() replays backlog to late subscribers. If client reuses
		// a JSON-RPC id, a replayed historical response could match and cause stale
		// results. Drain replayed messages before sending this request.
		for {
			select {
			case <-ch:
				continue
			default:
				goto drained
			}
		}
	drained:
		if err := sess.writeLine(line); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}

		deadline := time.NewTimer(g.defaultBackendTimeout)
		defer deadline.Stop()
		for {
			select {
			case <-r.Context().Done():
				http.Error(w, "request canceled", http.StatusBadGateway)
				return
			case <-deadline.C:
				g.logger.Printf("direct-rpc timeout mcp=%s method=%q id=%s session=%s", mcpName, method, idKey, sess.id)
				http.Error(w, "timeout waiting for json-rpc response", http.StatusGatewayTimeout)
				return
			case msg, ok := <-ch:
				if !ok {
					http.Error(w, "session closed", http.StatusBadGateway)
					return
				}
				if msg.event != "response" {
					continue
				}
				rid, ok := parseJSONRPCID(msg.data)
				if !ok || rid != idKey {
					continue
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				restored := sess.restoreJSONRPCIDIfPossible(msg.data)
				_, _ = w.Write([]byte(restored))
				g.logger.Printf("direct-rpc responded mcp=%s method=%q id=%s session=%s", mcpName, method, idKey, sess.id)
				return
			}
		}
	}

	backend, err := g.resolveBackend(mcpName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if backend.Type == "http" {
		g.handleDirectHTTPForward(w, r, backend, line)
		return
	}

	timeout := backend.Timeout
	if timeout <= 0 {
		timeout = g.defaultBackendTimeout
	}

	cmd := exec.Command(backend.Command, backend.Args...)
	cmd.Env = buildEnv(backend.Env, backend.EnvInherit)
	if backend.WorkingDir != "" {
		cmd.Dir = backend.WorkingDir
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		http.Error(w, "stdin pipe failed", http.StatusInternalServerError)
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		http.Error(w, "stdout pipe failed", http.StatusInternalServerError)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		http.Error(w, "stderr pipe failed", http.StatusInternalServerError)
		return
	}

	if err := cmd.Start(); err != nil {
		http.Error(w, "process start failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer killCmd(cmd, 2*time.Second)

	stderrCh := make(chan string, 128)
	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
		for sc.Scan() {
			select {
			case stderrCh <- sc.Text():
			default:
			}
		}
		close(stderrCh)
	}()

	if _, err := io.WriteString(stdin, line+"\n"); err != nil {
		http.Error(w, "write stdin failed", http.StatusBadGateway)
		return
	}

	resCh := make(chan struct {
		line string
		err  error
	}, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
		if sc.Scan() {
			resCh <- struct {
				line string
				err  error
			}{line: strings.TrimSpace(sc.Text())}
			return
		}
		resCh <- struct {
			line string
			err  error
		}{err: sc.Err()}
	}()

	select {
	case res := <-resCh:
		if res.line == "" {
			detail := "no response from mcp"
			if res.err != nil {
				detail += "; stdout scan error: " + res.err.Error()
			}
			if s := drainLines(stderrCh, 8); s != "" {
				detail += "; stderr: " + s
			}
			http.Error(w, detail, http.StatusBadGateway)
			return
		}
		_ = stdin.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(res.line))
	case <-time.After(timeout):
		detail := "timeout waiting for mcp response"
		if s := drainLines(stderrCh, 8); s != "" {
			detail += "; stderr: " + s
		}
		http.Error(w, detail, http.StatusGatewayTimeout)
		return
	}
}

func (g *Gateway) handleDirectHTTPForward(w http.ResponseWriter, r *http.Request, backend ResolvedBackend, line string) {
	url := strings.TrimSpace(backend.URL)
	if url == "" {
		http.Error(w, "http backend url is empty", http.StatusBadGateway)
		return
	}

	timeout := backend.Timeout
	if timeout <= 0 {
		timeout = g.defaultBackendTimeout
	}

	client := newHTTPClient(timeout)
	retryCfg := loadHTTPRetryConfigFromEnv("MCP_GATEWAY_HTTP_FORWARD", 8, 400*time.Millisecond, 8*time.Second)

	type forwardResult struct {
		Status int
		Body   []byte
	}

	res, err := doWithHTTPRetry(retryCfg, func() (forwardResult, int, error) {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, url, bytes.NewBufferString(line))
		if err != nil {
			return forwardResult{}, 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		applyHeaders(req, mergeHeaders(backend.Headers, extractForwardHeaders(r)))

		resp, err := client.Do(req)
		if err != nil {
			return forwardResult{}, 0, fmt.Errorf("forward request failed: %w", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		if err != nil {
			return forwardResult{}, resp.StatusCode, fmt.Errorf("failed to read forward response: %w", err)
		}
		// Retry for 5xx/429 handled by doWithHTTPRetry via status.
		if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && resp.StatusCode <= 599) {
			return forwardResult{}, resp.StatusCode, fmt.Errorf("forward response status=%d", resp.StatusCode)
		}
		return forwardResult{Status: resp.StatusCode, Body: body}, resp.StatusCode, nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(res.Status)
	_, _ = w.Write(res.Body)
}

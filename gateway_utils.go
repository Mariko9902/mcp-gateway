package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

func mustUserHomeDir() string {
	h, _ := os.UserHomeDir()
	if h == "" {
		return "/"
	}
	return h
}

func extractForwardHeaders(r *http.Request) map[string]string {
	if r == nil {
		return nil
	}
	out := map[string]string{}
	for k, vs := range r.Header {
		if len(vs) == 0 {
			continue
		}
		ck := http.CanonicalHeaderKey(k)
		if strings.EqualFold(ck, "Authorization") {
			continue
		}
		if strings.EqualFold(ck, "X-API-Key") {
			continue
		}
		out[ck] = strings.Join(vs, ",")
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func disabledToolsFromHeader(r *http.Request) []string {
	if r == nil {
		return nil
	}
	v := strings.TrimSpace(r.Header.Get("Disabled-Tools"))
	if v == "" {
		v = strings.TrimSpace(r.Header.Get("X-Disabled-Tools"))
	}
	if v == "" {
		v = strings.TrimSpace(r.Header.Get("disabledTools"))
	}
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func injectDisabledToolsIfNeeded(line string, r *http.Request) string {
	line = strings.TrimSpace(line)
	if line == "" || r == nil {
		return line
	}
	disabled := disabledToolsFromHeader(r)
	if len(disabled) == 0 {
		return line
	}
	if parseJSONRPCMethod(line) != "tools/list" {
		return line
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return line
	}
	pm, _ := m["params"].(map[string]any)
	if pm == nil {
		pm = map[string]any{}
		m["params"] = pm
	}
	if v, ok := pm["disabledTools"]; ok && v != nil {
		return line
	}
	pm["disabledTools"] = disabled

	bs, err := json.Marshal(m)
	if err != nil {
		return line
	}
	return strings.TrimSpace(string(bs))
}

func mergeHeaders(base map[string]string, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := map[string]string{}
	for k, v := range base {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out[http.CanonicalHeaderKey(k)] = v
	}
	for k, v := range override {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out[http.CanonicalHeaderKey(k)] = v
	}
	return out
}

func applyHeaders(req *http.Request, headers map[string]string) {
	if req == nil || len(headers) == 0 {
		return
	}
	for k, v := range headers {
		if strings.TrimSpace(k) == "" {
			continue
		}
		req.Header.Set(http.CanonicalHeaderKey(k), v)
	}
}

func splitPath(p string) []string {
	p = strings.TrimSpace(p)
	if p == "" || p == "/" {
		return nil
	}
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

func newSessionID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func classifyJSONRPCEvent(raw string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return "message"
	}
	if v, ok := m["id"]; ok && v != nil {
		return "response"
	}
	return "notification"
}

func buildEnv(extra map[string]string, inherit bool) []string {
	// NOTE: it's possible that the parent environment already contains the same
	// key as `extra` (e.g. SESSION_MCP_GATEWAY_URL). If we just append, the child
	// process may observe the *old* value depending on libc/runtime getenv lookup.
	// To make behavior deterministic, de-duplicate keys and let `extra` override.
	var env []string
	index := map[string]int{}

	appendKV := func(k, v string) {
		kv := k + "=" + v
		if i, ok := index[k]; ok {
			env[i] = kv
			return
		}
		index[k] = len(env)
		env = append(env, kv)
	}

	if inherit {
		for _, kv := range os.Environ() {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			k = strings.TrimSpace(k)
			if k == "" {
				continue
			}
			appendKV(k, v)
		}
	}
	for k, v := range extra {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		appendKV(k, v)
	}
	return env
}

func killCmd(cmd *exec.Cmd, wait time.Duration) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	done := make(chan struct{}, 1)
	go func() {
		_ = cmd.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(wait):
	}
}

func drainLines(ch <-chan string, maxLines int) string {
	if maxLines <= 0 {
		maxLines = 8
	}
	lines := make([]string, 0, maxLines)
	for len(lines) < maxLines {
		select {
		case s, ok := <-ch:
			if !ok {
				return strings.Join(lines, " | ")
			}
			if s = strings.TrimSpace(s); s != "" {
				lines = append(lines, s)
			}
		default:
			return strings.Join(lines, " | ")
		}
	}
	return strings.Join(lines, " | ")
}

package main

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func (g *Gateway) swarmRootDir() string {
	v := strings.TrimSpace(os.Getenv("MCP_GATEWAY_SWARM_ROOT"))
	if v != "" {
		return v
	}
	// fallback to swarm-mcp default data root
	v = strings.TrimSpace(os.Getenv("SWARM_MCP_ROOT"))
	if v != "" {
		return v
	}
	return filepath.Join(mustUserHomeDir(), ".swarm-mcp")
}

func (g *Gateway) handleSwarmMCPDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"error":"method_not_allowed"}`))
		return
	}

	q := r.URL.Query()
	rel := strings.TrimSpace(q.Get("path"))
	if rel == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"path_required"}`))
		return
	}
	// allow callers to pass already-escaped paths
	if decoded, err := url.QueryUnescape(rel); err == nil {
		rel = decoded
	}

	root := g.swarmRootDir()
	abs, err := safeJoin(root, rel)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_path"}`))
		return
	}

	f, err := os.Open(abs)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not_found"}`))
		return
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil || st.IsDir() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not_found"}`))
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+sanitizeFilename(filepath.Base(abs))+"\"")
	w.Header().Set("Content-Length", intToString(st.Size()))

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
}

func safeJoin(root, rel string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", errors.New("empty root")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}

	rel = strings.ReplaceAll(rel, "\\", "/")
	rel = strings.TrimSpace(rel)
	rel = strings.TrimPrefix(rel, "/")
	clean := filepath.Clean(rel)
	if clean == "." || clean == "" {
		return "", errors.New("empty path")
	}
	if strings.HasPrefix(clean, "..") {
		return "", errors.New("path traversal")
	}

	abs := filepath.Join(rootAbs, clean)
	abs, err = filepath.Abs(abs)
	if err != nil {
		return "", err
	}
	sep := string(filepath.Separator)
	if abs != rootAbs && !strings.HasPrefix(abs, rootAbs+sep) {
		return "", errors.New("escape root")
	}

	// optional scope allowlist for safety
	allowedPrefixes := []string{
		"issues" + sep,
		"docs" + sep,
		"trace" + sep,
		"locks" + sep,
		"workers" + sep,
	}
	relOS := filepath.Clean(clean)
	if !strings.HasSuffix(relOS, sep) {
		relOS = relOS + sep
	}
	ok := false
	for _, p := range allowedPrefixes {
		if strings.HasPrefix(relOS, p) {
			ok = true
			break
		}
	}
	if !ok {
		return "", errors.New("path not allowed")
	}

	return abs, nil
}

func sanitizeFilename(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "\n", "")
	name = strings.ReplaceAll(name, "\r", "")
	name = strings.ReplaceAll(name, "\"", "")
	if name == "" {
		return "download"
	}
	return name
}

func intToString(n int64) string {
	// avoid strconv import for this small helper
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [32]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + (n % 10))
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

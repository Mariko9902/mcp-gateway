package main

import (
	"net/http"
	"os"
	"strings"
)

func (g *Gateway) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	token := strings.TrimSpace(os.Getenv("MCP_GATEWAY_TOKEN"))
	if token == "" {
		return true
	}

	apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if apiKey != "" && apiKey == token {
		return true
	}

	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
		if strings.TrimSpace(auth[7:]) == token {
			return true
		}
	}

	hasAuth := strings.TrimSpace(r.Header.Get("Authorization")) != ""
	hasAPIKey := strings.TrimSpace(r.Header.Get("X-API-Key")) != ""
	g.logger.Printf("unauthorized request: method=%s path=%s remote=%s ua=%q has_auth=%v has_api_key=%v", r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent(), hasAuth, hasAPIKey)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	return false
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.logger.Printf("request: method=%s path=%s remote=%s ua=%q", r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent())
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	if !g.requireAuth(w, r) {
		return
	}
	if strings.HasPrefix(r.URL.Path, "/mcp-files/") || strings.HasPrefix(r.URL.Path, "/files/") {
		parts := splitPath(r.URL.Path)
		if len(parts) == 3 && (parts[0] == "mcp-files" || parts[0] == "files") && parts[1] == "swarm-mcp" && parts[2] == "download" {
			g.handleSwarmMCPDownload(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	parts := splitPath(r.URL.Path)
	if len(parts) == 1 && parts[0] == "mcps" && r.Method == http.MethodGet {
		g.handleListMCPs(w, r)
		return
	}
	if len(parts) < 2 || parts[0] != "mcps" {
		http.NotFound(w, r)
		return
	}
	mcpName := parts[1]
	if mcpName == "session" {
		mcpName = "session-mcp"
	}
	if len(parts) == 2 && r.Method == http.MethodGet {
		g.handleCompatEvents(w, r, mcpName)
		return
	}

	if len(parts) == 2 && r.Method == http.MethodPost {
		g.handleDirectRPC(w, r, mcpName)
		return
	}
	if len(parts) == 3 && parts[2] == "session" && r.Method == http.MethodPost {
		g.handleCreateSession(w, r, mcpName)
		return
	}
	if len(parts) >= 4 && parts[2] == "session" {
		sid := parts[3]
		if len(parts) == 4 && r.Method == http.MethodDelete {
			g.handleDeleteSession(w, r, mcpName, sid)
			return
		}
		if len(parts) == 5 && parts[4] == "events" && r.Method == http.MethodGet {
			g.handleEvents(w, r, mcpName, sid)
			return
		}
		if len(parts) == 5 && parts[4] == "rpc" && r.Method == http.MethodPost {
			g.handleRPC(w, r, mcpName, sid)
			return
		}
	}
	http.NotFound(w, r)
}

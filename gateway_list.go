package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
)

func (g *Gateway) handleListMCPs(w http.ResponseWriter, r *http.Request) {
	routes := g.getRoutes()

	type routeInfo struct {
		Type    string            `json:"type"`
		Command string            `json:"command,omitempty"`
		URL     string            `json:"url,omitempty"`
		Env     map[string]string `json:"env,omitempty"`
	}

	configured := map[string]routeInfo{}
	for name, route := range routes.Routes {
		info := routeInfo{Type: route.Type}
		if route.Type == "local" {
			cmd := route.Command
			if !filepath.IsAbs(cmd) {
				cmd = filepath.Join(g.rootDir, cmd)
			}
			info.Command = cmd
			if len(route.Env) > 0 {
				info.Env = route.Env
			}
		} else if route.Type == "http" {
			info.URL = route.URL
		}
		configured[name] = info
	}

	discovered := []string{}
	entries, err := os.ReadDir(g.rootDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if _, already := configured[name]; already {
				continue
			}
			if b, err := g.resolveBackend(name); err == nil && b.Type == "local" {
				discovered = append(discovered, name)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"configured":  configured,
		"discovered":  discovered,
		"routes_file": g.routesPath,
	})
}

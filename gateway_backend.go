package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (g *Gateway) ReloadRoutes() error {
	if strings.TrimSpace(g.routesPath) == "" {
		return nil
	}
	cfg, err := LoadRoutesConfig(g.routesPath)
	if err != nil {
		return err
	}
	g.routesMu.Lock()
	g.routes = cfg
	g.routesMu.Unlock()
	g.logger.Printf("routes reloaded from %s (%d routes)", g.routesPath, len(cfg.Routes))
	return nil
}

func (g *Gateway) getRoutes() RoutesConfig {
	g.routesMu.RLock()
	defer g.routesMu.RUnlock()
	return g.routes
}

func (g *Gateway) resolveBackend(mcpName string) (ResolvedBackend, error) {
	name := strings.TrimSpace(mcpName)
	if name == "" {
		return ResolvedBackend{}, fmt.Errorf("unknown mcp: %s", mcpName)
	}

	routes := g.getRoutes()
	if r, ok := routes.Routes[name]; ok {
		inherit := r.EnvInherit == nil || *r.EnvInherit
		timeout := r.ParseTimeout(g.defaultBackendTimeout)
		switch r.Type {
		case "local":
			cmd := r.Command
			if !filepath.IsAbs(cmd) {
				cmd = filepath.Join(g.rootDir, cmd)
			}
			return ResolvedBackend{
				Type:       "local",
				Command:    cmd,
				Args:       r.Args,
				Env:        r.Env,
				EnvInherit: inherit,
				WorkingDir: r.WorkingDir,
				Timeout:    timeout,
			}, nil
		case "http":
			return ResolvedBackend{Type: "http", URL: r.URL, Timeout: timeout, Headers: r.Headers}, nil
		default:
			return ResolvedBackend{}, fmt.Errorf("unknown mcp: %s", mcpName)
		}
	}

	// default local discovery: rootDir/{name}/bin/{name}
	bin := filepath.Join(g.rootDir, name, "bin", name)
	if stat, err := os.Stat(bin); err == nil && !stat.IsDir() {
		return ResolvedBackend{Type: "local", Command: bin, EnvInherit: true}, nil
	}

	// fallback: single executable in bin/
	binDir := filepath.Join(g.rootDir, name, "bin")
	entries, err := os.ReadDir(binDir)
	if err != nil {
		return ResolvedBackend{}, fmt.Errorf("unknown mcp: %s", mcpName)
	}
	candidates := make([]string, 0, 2)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fname := e.Name()
		if strings.HasPrefix(fname, ".") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.Mode()&0111 == 0 {
			continue
		}
		candidates = append(candidates, filepath.Join(binDir, fname))
	}
	if len(candidates) == 1 {
		return ResolvedBackend{Type: "local", Command: candidates[0], EnvInherit: true}, nil
	}
	return ResolvedBackend{}, fmt.Errorf("unknown mcp: %s", mcpName)
}

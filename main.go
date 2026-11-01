package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:15410", "listen address")
	root := flag.String("root", "", "mcp root dir, default: ~/Coding/mcp")
	routesPath := flag.String("routes", "", "routes yaml file")
	statePath := flag.String("state", "./mcp-gateway.state.json", "state file for sessions")
	idle := flag.Duration("idle", 0, "session idle timeout")
	flag.Parse()
	_ = godotenv.Load()

	logger := log.New(os.Stderr, "mcp-gateway: ", log.LstdFlags|log.LUTC)

	routesCfg, err := LoadRoutesConfig(*routesPath)
	if err != nil {
		logger.Printf("failed to load routes: %v", err)
		os.Exit(1)
	}

	gw := NewGateway(*root, logger, *idle, *routesPath, *statePath, routesCfg)
	_ = gw.ReloadRoutes()
	if err := gw.LoadState(); err != nil {
		logger.Printf("load state failed: %v", err)
	}

	// Hot reload routes on SIGHUP
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGHUP)
	go func() {
		for range sigCh {
			if err := gw.ReloadRoutes(); err != nil {
				logger.Printf("reload routes failed: %v", err)
			}
		}
	}()
	logger.Printf("listening on %s", *listen)
	if err := http.ListenAndServe(*listen, gw); err != nil {
		logger.Printf("server stopped with error: %v", err)
		os.Exit(1)
	}
}

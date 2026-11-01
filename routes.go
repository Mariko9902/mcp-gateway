package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type RoutesConfig struct {
	Routes map[string]Route `yaml:"routes"`
}

type Route struct {
	Type string `yaml:"type"` // local|http

	// local backend
	Command    string            `yaml:"command"` // absolute path OR relative (resolved under rootDir)
	Args       []string          `yaml:"args"`
	Env        map[string]string `yaml:"env"`
	WorkingDir string            `yaml:"working_dir"`
	EnvInherit *bool             `yaml:"env_inherit"` // default true; set false for isolated env

	// shared
	Timeout string `yaml:"timeout"` // Go duration string, e.g. "30s", "2m"

	// http backend
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
}

func (r Route) ParseTimeout(defaultTimeout time.Duration) time.Duration {
	if r.Timeout == "" {
		return defaultTimeout
	}
	d, err := time.ParseDuration(r.Timeout)
	if err != nil || d <= 0 {
		return defaultTimeout
	}
	return d
}

func LoadRoutesConfig(path string) (RoutesConfig, error) {
	if path == "" {
		return RoutesConfig{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return RoutesConfig{}, err
	}
	var cfg RoutesConfig
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return RoutesConfig{}, err
	}
	if cfg.Routes == nil {
		cfg.Routes = map[string]Route{}
	}

	for name, r := range cfg.Routes {
		if r.Type == "" {
			return RoutesConfig{}, fmt.Errorf("route '%s': type is required", name)
		}
		if r.Type != "local" && r.Type != "http" {
			return RoutesConfig{}, fmt.Errorf("route '%s': invalid type '%s'", name, r.Type)
		}
		if r.Type == "local" && r.Command == "" {
			return RoutesConfig{}, fmt.Errorf("route '%s': command is required for local backend", name)
		}
		if r.Type == "http" && r.URL == "" {
			return RoutesConfig{}, fmt.Errorf("route '%s': url is required for http backend", name)
		}
		if r.Timeout != "" {
			if _, err := time.ParseDuration(r.Timeout); err != nil {
				return RoutesConfig{}, fmt.Errorf("route '%s': invalid timeout %q: %v", name, r.Timeout, err)
			}
		}
	}

	return cfg, nil
}

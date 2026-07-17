package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
)

// config holds the runtime configuration for the Strangler Fig proxy.
type config struct {
	port                   string
	monolithURL            *url.URL
	moviesURL              *url.URL
	eventsURL              *url.URL
	gradualMigration       bool
	moviesMigrationPercent int
}

// loadConfig builds a config from environment variables.
// Missing or unparseable upstream URLs are fatal (returned as an error).
func loadConfig() (*config, error) {
	cfg := &config{
		port: getEnv("PORT", "8000"),
	}

	var err error
	if cfg.monolithURL, err = parseURL("MONOLITH_URL"); err != nil {
		return nil, err
	}
	if cfg.moviesURL, err = parseURL("MOVIES_SERVICE_URL"); err != nil {
		return nil, err
	}
	if cfg.eventsURL, err = parseURL("EVENTS_SERVICE_URL"); err != nil {
		return nil, err
	}

	if raw := os.Getenv("GRADUAL_MIGRATION"); raw != "" {
		if b, perr := strconv.ParseBool(raw); perr == nil {
			cfg.gradualMigration = b
		} else {
			log.Printf("proxy: cannot parse GRADUAL_MIGRATION=%q, defaulting to false", raw)
		}
	}

	cfg.moviesMigrationPercent = 0
	if raw := os.Getenv("MOVIES_MIGRATION_PERCENT"); raw != "" {
		if n, perr := strconv.Atoi(raw); perr == nil {
			cfg.moviesMigrationPercent = clampPercent(n)
		} else {
			log.Printf("proxy: cannot parse MOVIES_MIGRATION_PERCENT=%q, defaulting to 0", raw)
		}
	}

	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseURL(key string) (*url.URL, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return nil, fmt.Errorf("required environment variable %s is not set", key)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL in %s=%q: %w", key, raw, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("URL in %s=%q must include scheme and host", key, raw)
	}
	return u, nil
}

func clampPercent(n int) int {
	if n < 0 {
		return 0
	}
	if n > 100 {
		return 100
	}
	return n
}

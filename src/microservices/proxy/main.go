package main

import (
	"log"
	"net/http"
	"time"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("proxy: configuration error: %v", err)
	}

	rt := newRouter(cfg)

	log.Printf("proxy: starting on :%s (gradualMigration=%t, moviesMigrationPercent=%d)",
		cfg.port, cfg.gradualMigration, cfg.moviesMigrationPercent)
	log.Printf("proxy: monolith=%s movies=%s events=%s",
		cfg.monolithURL, cfg.moviesURL, cfg.eventsURL)

	srv := &http.Server{
		Addr:              ":" + cfg.port,
		Handler:           rt,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

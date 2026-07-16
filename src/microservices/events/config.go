package main

import (
	"log"
	"os"
	"strings"
)

// config holds runtime configuration for the events service.
type config struct {
	port    string
	brokers []string
}

// loadConfig reads configuration from the environment. KAFKA_BROKERS is
// required; its absence is fatal.
func loadConfig() *config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		log.Fatal("events: KAFKA_BROKERS environment variable is required")
	}

	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		log.Fatal("events: KAFKA_BROKERS did not contain any broker addresses")
	}

	return &config{port: port, brokers: brokers}
}

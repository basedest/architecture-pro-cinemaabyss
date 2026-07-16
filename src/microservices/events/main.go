package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	prod := newProducer(cfg.brokers)
	defer prod.close()

	startConsumers(ctx, cfg.brokers)

	srv := &server{producer: prod}
	mux := http.NewServeMux()
	srv.routes(mux)

	httpServer := &http.Server{
		Addr:              ":" + cfg.port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("events: listening on :%s, brokers=%v", cfg.port, cfg.brokers)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("events: server error: %v", err)
	}
	log.Print("events: shut down cleanly")
	_ = os.Stdout.Sync()
}

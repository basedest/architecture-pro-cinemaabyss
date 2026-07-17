package main

import (
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// newProxy builds a reverse proxy to a single upstream host with a JSON 502
// error handler so upstream outages never leak the default HTML error page.
func newProxy(target *url.URL) *httputil.ReverseProxy {
	p := httputil.NewSingleHostReverseProxy(target)
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy: upstream error for %s %s -> %s: %v", r.Method, r.URL.Path, target.Host, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"bad gateway"}`))
	}
	return p
}

// router is the single Strangler Fig handler. It inspects the request path and
// routes to the monolith, the movies service, or the events service.
type router struct {
	cfg      *config
	monolith *httputil.ReverseProxy
	movies   *httputil.ReverseProxy
	events   *httputil.ReverseProxy
}

func newRouter(cfg *config) *router {
	return &router{
		cfg:      cfg,
		monolith: newProxy(cfg.monolithURL),
		movies:   newProxy(cfg.moviesURL),
		events:   newProxy(cfg.eventsURL),
	}
}

func (rt *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	if path == "/health" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
		return
	}

	switch {
	case path == "/api/events" || strings.HasPrefix(path, "/api/events/"):
		rt.route(w, r, rt.events, rt.cfg.eventsURL.Host)

	case path == "/api/movies" || strings.HasPrefix(path, "/api/movies/"):
		if rt.cfg.gradualMigration && rand.IntN(100) < rt.cfg.moviesMigrationPercent {
			rt.route(w, r, rt.movies, rt.cfg.moviesURL.Host)
		} else {
			rt.route(w, r, rt.monolith, rt.cfg.monolithURL.Host)
		}

	default:
		rt.route(w, r, rt.monolith, rt.cfg.monolithURL.Host)
	}
}

func (rt *router) route(w http.ResponseWriter, r *http.Request, p *httputil.ReverseProxy, targetHost string) {
	log.Printf("proxy: %s %s -> %s", r.Method, r.URL.Path, targetHost)
	p.ServeHTTP(w, r)
}

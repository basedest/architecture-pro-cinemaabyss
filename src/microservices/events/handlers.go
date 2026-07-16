package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ---- Event payload types (mirrors api-specification.yaml) ----

type movieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type userEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type paymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type,omitempty"`
}

// ---- Envelope + response types ----

type event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type eventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     event  `json:"event"`
}

// server wires the producer into the HTTP handlers.
type server struct {
	producer *producer
}

func (s *server) routes(mux *http.ServeMux) {
	mux.HandleFunc("/api/events/health", s.handleHealth)
	mux.HandleFunc("/api/events/movie", s.handleMovie)
	mux.HandleFunc("/api/events/user", s.handleUser)
	mux.HandleFunc("/api/events/payment", s.handlePayment)
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (s *server) handleMovie(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var ev movieEvent
	if err := decode(r, &ev); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if ev.MovieID <= 0 || ev.Title == "" || ev.Action == "" {
		writeError(w, http.StatusBadRequest, "movie_id, title and action are required")
		return
	}
	s.publish(w, r, "movie", topicMovie, ev.MovieID, ev)
}

func (s *server) handleUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var ev userEvent
	if err := decode(r, &ev); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if ev.UserID <= 0 || ev.Action == "" || ev.Timestamp.IsZero() {
		writeError(w, http.StatusBadRequest, "user_id, action and timestamp are required")
		return
	}
	s.publish(w, r, "user", topicUser, ev.UserID, ev)
}

func (s *server) handlePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var ev paymentEvent
	if err := decode(r, &ev); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if ev.PaymentID <= 0 || ev.UserID <= 0 || ev.Amount <= 0 || ev.Status == "" || ev.Timestamp.IsZero() {
		writeError(w, http.StatusBadRequest, "payment_id, user_id, amount, status and timestamp are required")
		return
	}
	s.publish(w, r, "payment", topicPayment, ev.PaymentID, ev)
}

// publish wraps a decoded payload in an Event envelope, produces it to Kafka,
// and writes the 201 EventResponse.
func (s *server) publish(w http.ResponseWriter, r *http.Request, kind, topic string, entityID int, payload interface{}) {
	e := event{
		ID:        fmt.Sprintf("%s-%d-%d", kind, entityID, time.Now().UnixNano()),
		Type:      kind,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}

	body, err := json.Marshal(e)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode event")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	partition, offset, err := s.producer.produce(ctx, topic, body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to publish event: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, eventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     e,
	})
}

// ---- helpers ----

func decode(r *http.Request, dst interface{}) error {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

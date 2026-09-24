package api

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ddchencm/ha/internal/ha"
)

type Server struct{ controller *ha.Controller }

func NewServer(controller *ha.Controller) *Server { return &Server{controller: controller} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /api/v1/status", s.status)
	mux.HandleFunc("POST /api/v1/switchover", s.switchover)
	mux.HandleFunc("POST /api/v1/auto-failover", s.autoFailover)
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	state, statuses, task, err := s.controller.Status()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cluster": state, "nodes": statuses, "last_task": task})
}

func (s *Server) switchover(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Target string `json:"target"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&request)
	}
	task, err := s.controller.Switch(r.Context(), request.Target, false)
	if err != nil {
		writeJSON(w, http.StatusConflict, task)
		return
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) autoFailover(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, err)
		return
	}
	if err := s.controller.SetAutoFailover(r.Context(), request.Enabled); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, request)
}

func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

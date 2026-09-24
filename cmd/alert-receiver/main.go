package main

import (
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

type event struct {
	Severity string    `json:"severity"`
	Title    string    `json:"title"`
	Message  string    `json:"message"`
	At       time.Time `json:"at"`
}

func main() {
	addr := flag.String("listen", "127.0.0.1:9099", "HTTP listen address")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var mu sync.Mutex
	events := make([]event, 0, 32)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var payload event
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		payload.At = time.Now().UTC()
		mu.Lock()
		events = append(events, payload)
		mu.Unlock()
		logger.Info("alert received", "severity", payload.Severity, "title", payload.Title)
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(events)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	server := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	logger.Info("validation alert receiver started", "addr", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("alert receiver stopped", "error", err)
		os.Exit(1)
	}
}

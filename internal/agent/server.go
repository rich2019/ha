package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ddchencm/ha/internal/model"
)

type SwitchVerifier interface {
	VerifySwitchOperation(context.Context, model.SwitchAuthorization) error
}

type MySQLOperator interface {
	Status(context.Context) model.NodeStatus
	ExecutedGTID(context.Context) (string, error)
	WaitForGTID(context.Context, string, int) error
	Demote(context.Context) error
	Promote(context.Context) error
	ReconfigureReplica(context.Context, model.AgentNodeConfig) error
	Close() error
}

type Server struct {
	nodeID string
	db     MySQLOperator
	store  SwitchVerifier
}

func NewServer(nodeID string, db MySQLOperator, stateStore SwitchVerifier) (*Server, error) {
	if nodeID == "" || db == nil || stateStore == nil {
		return nil, errors.New("node ID, MySQL client and operation verifier are required")
	}
	return &Server{nodeID: nodeID, db: db, store: stateStore}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/v1/status", s.status)
	mux.HandleFunc("GET /api/v1/gtid", s.gtid)
	mux.HandleFunc("POST /api/v1/wait-gtid", s.waitGTID)
	mux.HandleFunc("POST /api/v1/demote", s.demote)
	mux.HandleFunc("POST /api/v1/promote", s.promote)
	mux.HandleFunc("POST /api/v1/replica", s.replica)
	return mux
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.db.Status(r.Context()))
}

func (s *Server) gtid(w http.ResponseWriter, r *http.Request) {
	gtid, err := s.db.ExecutedGTID(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"gtid": gtid})
}

func (s *Server) waitGTID(w http.ResponseWriter, r *http.Request) {
	var request model.WaitGTIDRequest
	if err := decode(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.authorize(r, request.Authorization); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	if request.TimeoutSeconds < 1 || request.TimeoutSeconds > 60 || strings.TrimSpace(request.GTID) == "" {
		writeError(w, http.StatusBadRequest, errors.New("GTID and timeout from 1 to 60 seconds are required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(request.TimeoutSeconds+2)*time.Second)
	defer cancel()
	if err := s.db.WaitForGTID(ctx, request.GTID, request.TimeoutSeconds); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "caught_up"})
}

func (s *Server) demote(w http.ResponseWriter, r *http.Request) {
	var request model.OperationRequest
	if err := decode(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.authorize(r, request.Authorization); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	if err := s.db.Demote(r.Context()); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "read_only"})
}

func (s *Server) promote(w http.ResponseWriter, r *http.Request) {
	var request model.OperationRequest
	if err := decode(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.authorize(r, request.Authorization); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	status := s.db.Status(r.Context())
	if !status.Healthy || status.Role != model.RoleReplica || status.Replica == nil || !status.Replica.IOThreadRunning || !status.Replica.SQLThreadRunning || status.Replica.SecondsBehind == nil || *status.Replica.SecondsBehind != 0 {
		writeError(w, http.StatusConflict, errors.New("agent refuses promotion: node is not a healthy caught-up replica"))
		return
	}
	if err := s.db.Promote(r.Context()); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	status = s.db.Status(r.Context())
	if !status.Healthy || status.Role != model.RolePrimary {
		writeError(w, http.StatusConflict, errors.New("promoted node did not become writable"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "primary"})
}

func (s *Server) replica(w http.ResponseWriter, r *http.Request) {
	var request model.ReplicaRequest
	if err := decode(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.authorize(r, request.Authorization); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	if request.Source.ID == "" || request.Source.ID == s.nodeID || request.Source.Address == "" {
		writeError(w, http.StatusBadRequest, errors.New("a different source node and address are required"))
		return
	}
	if err := s.db.ReconfigureReplica(r.Context(), request.Source); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		status := s.db.Status(r.Context())
		if status.Healthy && status.Role == model.RoleReplica && status.Replica != nil && status.Replica.IOThreadRunning && status.Replica.SQLThreadRunning {
			writeJSON(w, http.StatusOK, map[string]string{"status": "replica"})
			return
		}
		select {
		case <-r.Context().Done():
			writeError(w, http.StatusRequestTimeout, r.Context().Err())
			return
		case <-deadline.C:
			writeError(w, http.StatusConflict, fmt.Errorf("replica reconfiguration did not become healthy: %s", status.Error))
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) authorize(r *http.Request, auth model.SwitchAuthorization) error {
	if auth.OperationID == "" {
		return errors.New("operation ID is required")
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	return s.store.VerifySwitchOperation(ctx, auth)
}

func decode(r *http.Request, out any) error {
	if r.Body == nil {
		return errors.New("request body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ddchencm/ha/internal/model"
	"github.com/ddchencm/ha/internal/store"
)

type fakeMySQL struct {
	status     model.NodeStatus
	promotions int
}

func (f *fakeMySQL) Status(context.Context) model.NodeStatus      { return f.status }
func (*fakeMySQL) ExecutedGTID(context.Context) (string, error)   { return "uuid:1-3", nil }
func (*fakeMySQL) WaitForGTID(context.Context, string, int) error { return nil }
func (*fakeMySQL) Demote(context.Context) error                   { return nil }
func (f *fakeMySQL) Promote(context.Context) error {
	f.promotions++
	f.status = model.NodeStatus{Reachable: true, Healthy: true, Role: model.RolePrimary}
	return nil
}
func (*fakeMySQL) ReconfigureReplica(context.Context, model.AgentNodeConfig) error { return nil }
func (*fakeMySQL) Close() error                                                    { return nil }

func TestAgentRejectsStaleSwitchOperation(t *testing.T) {
	stateStore := store.NewMemoryStore("test")
	auth := model.SwitchAuthorization{OperationID: "active-op", FromEpoch: 0, ToEpoch: 1}
	_, release, err := stateStore.AcquireSwitchLock(context.Background(), "controller", time.Second, auth)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	db := &fakeMySQL{status: model.NodeStatus{Reachable: true, Healthy: true, Role: model.RoleReplica, Replica: &model.ReplicaStatus{IOThreadRunning: true, SQLThreadRunning: true, SecondsBehind: ptr(0)}}}
	service, err := NewServer("db-1", db, stateStore)
	if err != nil {
		t.Fatal(err)
	}
	wrong := auth
	wrong.OperationID = "stale-op"
	body, _ := json.Marshal(model.OperationRequest{Authorization: wrong})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/promote", strings.NewReader(string(body)))
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("got status %d, want 403: %s", response.Code, response.Body.String())
	}
	if db.promotions != 0 {
		t.Fatal("agent executed a promotion for a stale operation")
	}
}

func TestAgentPromotesOnlyCaughtUpReplicaWithActiveOperation(t *testing.T) {
	stateStore := store.NewMemoryStore("test")
	auth := model.SwitchAuthorization{OperationID: "active-op", FromEpoch: 0, ToEpoch: 1}
	_, release, err := stateStore.AcquireSwitchLock(context.Background(), "controller", time.Second, auth)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	db := &fakeMySQL{status: model.NodeStatus{Reachable: true, Healthy: true, Role: model.RoleReplica, Replica: &model.ReplicaStatus{IOThreadRunning: true, SQLThreadRunning: true, SecondsBehind: ptr(0)}}}
	service, err := NewServer("db-1", db, stateStore)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(model.OperationRequest{Authorization: auth})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/promote", strings.NewReader(string(body)))
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200: %s", response.Code, response.Body.String())
	}
	if db.promotions != 1 || db.status.Role != model.RolePrimary {
		t.Fatalf("promotion was not applied: %+v", db)
	}
}

func ptr(value int64) *int64 { return &value }

package ha

import (
	"context"
	"testing"

	"github.com/ddchencm/ha/internal/config"
	"github.com/ddchencm/ha/internal/model"
	"github.com/ddchencm/ha/internal/store"
)

func TestChooseTargetPrefersLowestLag(t *testing.T) {
	c := &Controller{cfg: config.Config{MaxReplicaLagSeconds: 30}, statuses: map[string]model.NodeStatus{
		"primary":   {Role: model.RolePrimary, Healthy: true},
		"replica-a": {Role: model.RoleReplica, Healthy: true, Replica: &model.ReplicaStatus{SecondsBehind: ptr(int64(8))}},
		"replica-b": {Role: model.RoleReplica, Healthy: true, Replica: &model.ReplicaStatus{SecondsBehind: ptr(int64(2))}},
	}}
	got, err := c.chooseTarget("primary")
	if err != nil || got != "replica-b" {
		t.Fatalf("got target=%q err=%v", got, err)
	}
}

func TestChooseTargetRejectsLaggingReplica(t *testing.T) {
	c := &Controller{cfg: config.Config{MaxReplicaLagSeconds: 30}, statuses: map[string]model.NodeStatus{
		"replica": {Role: model.RoleReplica, Healthy: true, Replica: &model.ReplicaStatus{SecondsBehind: ptr(int64(31))}},
	}}
	if _, err := c.chooseTarget(""); err == nil {
		t.Fatal("expected no eligible replica")
	}
}

func TestMemoryStoreLockIsExclusive(t *testing.T) {
	s := store.NewMemoryStore("test")
	release, err := s.AcquireLock(context.Background(), "a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireLock(context.Background(), "b", 1); err == nil {
		t.Fatal("expected lock contention")
	}
	release()
	if _, err := s.AcquireLock(context.Background(), "b", 1); err != nil {
		t.Fatal(err)
	}
}

func ptr(value int64) *int64 { return &value }

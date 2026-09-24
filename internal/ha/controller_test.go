package ha

import (
	"context"
	"io"
	"log/slog"
	"testing"

	remoteagent "github.com/ddchencm/ha/internal/agent"
	"github.com/ddchencm/ha/internal/alert"
	"github.com/ddchencm/ha/internal/config"
	"github.com/ddchencm/ha/internal/fence"
	"github.com/ddchencm/ha/internal/model"
	"github.com/ddchencm/ha/internal/route"
	"github.com/ddchencm/ha/internal/store"
)

type fakeAgentNode struct {
	status       model.NodeStatus
	gtid         string
	promoteCalls int
}

func (f *fakeAgentNode) Status(context.Context) model.NodeStatus      { return f.status }
func (f *fakeAgentNode) ExecutedGTID(context.Context) (string, error) { return f.gtid, nil }
func (*fakeAgentNode) WaitForGTID(context.Context, model.SwitchAuthorization, string, int) error {
	return nil
}
func (f *fakeAgentNode) Demote(context.Context, model.SwitchAuthorization) error {
	f.status.ReadOnly, f.status.SuperReadOnly, f.status.Healthy = true, true, false
	f.status.Role, f.status.Replica = model.RoleReplica, nil
	return nil
}
func (f *fakeAgentNode) Promote(context.Context, model.SwitchAuthorization) error {
	f.promoteCalls++
	f.status.ReadOnly, f.status.SuperReadOnly, f.status.Healthy = false, false, true
	f.status.Role, f.status.Replica = model.RolePrimary, nil
	return nil
}
func (f *fakeAgentNode) ReconfigureReplica(context.Context, model.SwitchAuthorization, model.AgentNodeConfig) error {
	f.status.ReadOnly, f.status.SuperReadOnly, f.status.Healthy = true, true, true
	f.status.Role = model.RoleReplica
	f.status.Replica = &model.ReplicaStatus{IOThreadRunning: true, SQLThreadRunning: true, SecondsBehind: ptr(0)}
	return nil
}
func (*fakeAgentNode) Close() {}

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

func TestChooseDryRunTargetAllowsReplicaWithLostIOSourceThread(t *testing.T) {
	c := &Controller{statuses: map[string]model.NodeStatus{
		"primary": {Reachable: false, Role: model.RoleUnknown},
		"replica": {Reachable: true, Role: model.RoleReplica, Replica: &model.ReplicaStatus{IOThreadRunning: false, SQLThreadRunning: true}},
	}}
	got, err := c.chooseDryRunTarget("primary")
	if err != nil || got != "replica" {
		t.Fatalf("got target=%q err=%v", got, err)
	}
}

func TestReplicaIOFailureAlertHasDiagnosticAndWarningSeverity(t *testing.T) {
	severity, title, message := failureAlert(model.NodeStatus{Role: model.RoleReplica, Replica: &model.ReplicaStatus{IOThreadRunning: false, SQLThreadRunning: true, LastIOError: "source connection refused"}})
	if severity != "warning" || title != "MySQL replica IO thread unavailable" || message != "source connection refused" {
		t.Fatalf("unexpected alert: %s %s %s", severity, title, message)
	}
}

func TestMemoryStoreLockIsExclusive(t *testing.T) {
	s := store.NewMemoryStore("test")
	_, release, err := s.AcquireLock(context.Background(), "a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AcquireLock(context.Background(), "b", 1); err == nil {
		t.Fatal("expected lock contention")
	}
	release()
	if _, release, err := s.AcquireLock(context.Background(), "b", 1); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
}

func TestSwitchRejectedWhenControllerIsNotLeader(t *testing.T) {
	c := &Controller{}
	if _, err := c.Switch(context.Background(), "mysql-2", false); err == nil {
		t.Fatal("standby controller accepted a switchover")
	}
}

func TestManualSwitchoverAdvancesEpochAndReconfiguresAgents(t *testing.T) {
	stateStore := store.NewMemoryStore("test")
	if err := stateStore.PutCluster(context.Background(), model.ClusterState{Name: "test", Primary: "db-1", Epoch: 7}); err != nil {
		t.Fatal(err)
	}
	primary := &fakeAgentNode{gtid: "uuid:1-3", status: model.NodeStatus{Reachable: true, Healthy: true, Role: model.RolePrimary}}
	target := &fakeAgentNode{gtid: "uuid:1-3", status: model.NodeStatus{Reachable: true, Healthy: true, Role: model.RoleReplica, Replica: &model.ReplicaStatus{IOThreadRunning: true, SQLThreadRunning: true, SecondsBehind: ptr(0)}}}
	other := &fakeAgentNode{status: model.NodeStatus{Reachable: true, Healthy: true, Role: model.RoleReplica, Replica: &model.ReplicaStatus{IOThreadRunning: true, SQLThreadRunning: true, SecondsBehind: ptr(0)}}}
	nodes := map[string]remoteagent.Node{"db-1": primary, "db-2": target, "db-3": other}
	configs := map[string]model.AgentNodeConfig{"db-1": {ID: "db-1", Address: "10.0.0.1:3306"}, "db-2": {ID: "db-2", Address: "10.0.0.2:3306"}, "db-3": {ID: "db-3", Address: "10.0.0.3:3306"}}
	c := &Controller{cfg: config.Config{ExecuteActions: true, MaxReplicaLagSeconds: 30}, clients: nodes, configs: configs, store: stateStore, alerts: alert.NewManager(""), router: route.NoopManager{}, fencer: fence.NoopManager{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), statuses: map[string]model.NodeStatus{}, alertedNodes: map[string]bool{}, leaderCtx: context.Background()}
	c.leader.Store(true)
	task, err := c.Switch(context.Background(), "db-2", false)
	if err != nil {
		t.Fatalf("switchover failed: %v", err)
	}
	if task.State != "completed" || task.DryRun {
		t.Fatalf("unexpected task: %+v", task)
	}
	state, err := stateStore.GetCluster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Primary != "db-2" || state.Epoch != 8 {
		t.Fatalf("unexpected cluster state: %+v", state)
	}
	if primary.status.Role != model.RoleReplica || !primary.status.Healthy || other.status.Role != model.RoleReplica || !other.status.Healthy {
		t.Fatal("all non-primary agents must be healthy replicas after switch")
	}
}

func TestAutomaticDryRunReportsCandidateWithoutPromoting(t *testing.T) {
	stateStore := store.NewMemoryStore("test")
	if err := stateStore.PutCluster(context.Background(), model.ClusterState{Name: "test", Primary: "db-1", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	primary := &fakeAgentNode{status: model.NodeStatus{Reachable: false, Role: model.RoleUnknown}}
	primary.gtid = "uuid:1-3"
	candidate := &fakeAgentNode{status: model.NodeStatus{Reachable: true, Healthy: false, Role: model.RoleReplica, Replica: &model.ReplicaStatus{IOThreadRunning: false, SQLThreadRunning: true}}}
	nodes := map[string]remoteagent.Node{"db-1": primary, "db-2": candidate}
	configs := map[string]model.AgentNodeConfig{"db-1": {ID: "db-1", Address: "10.0.0.1:3306"}, "db-2": {ID: "db-2", Address: "10.0.0.2:3306"}}
	c := &Controller{cfg: config.Config{ExecuteActions: true, AutoFailoverExecute: false, MaxReplicaLagSeconds: 30}, clients: nodes, configs: configs, store: stateStore, alerts: alert.NewManager(""), router: route.NoopManager{}, fencer: fence.NoopManager{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), statuses: map[string]model.NodeStatus{}, alertedNodes: map[string]bool{}, leaderCtx: context.Background()}
	c.leader.Store(true)
	task, err := c.Switch(context.Background(), "db-2", true)
	if err != nil {
		t.Fatalf("dry-run failed: %v", err)
	}
	if !task.DryRun || task.State != "dry-run" {
		t.Fatalf("unexpected task: %+v", task)
	}
	if candidate.promoteCalls != 0 || primary.status.ReadOnly || primary.status.SuperReadOnly {
		t.Fatal("dry-run performed MySQL role mutations")
	}
	state, err := stateStore.GetCluster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Primary != "db-1" || state.Epoch != 2 {
		t.Fatalf("dry-run changed cluster state: %+v", state)
	}
}

func ptr(value int64) *int64 { return &value }

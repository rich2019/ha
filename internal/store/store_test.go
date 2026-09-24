package store

import (
	"context"
	"testing"
	"time"

	"github.com/ddchencm/ha/internal/model"
)

func TestPutClusterIfEpochRejectsStaleWriter(t *testing.T) {
	s := NewMemoryStore("test")
	initial, err := s.GetCluster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	initial.Primary = "mysql-1"
	initial.Epoch = 1
	if err := s.PutClusterIfEpoch(context.Background(), 0, initial); err != nil {
		t.Fatal(err)
	}
	stale := initial
	stale.Primary = "mysql-2"
	stale.Epoch = 2
	if err := s.PutClusterIfEpoch(context.Background(), 0, stale); err == nil {
		t.Fatal("stale epoch unexpectedly replaced cluster state")
	}
	current, _ := s.GetCluster(context.Background())
	if current.Primary != "mysql-1" || current.Epoch != 1 {
		t.Fatalf("unexpected state after rejected update: %+v", current)
	}
}

func TestMemoryStorePersistsNewestTasksFirst(t *testing.T) {
	s := NewMemoryStore("test")
	older := model.SwitchTask{ID: "older", StartedAt: time.Unix(1, 0)}
	newer := model.SwitchTask{ID: "newer", StartedAt: time.Unix(2, 0)}
	if err := s.PutTask(context.Background(), older); err != nil {
		t.Fatal(err)
	}
	if err := s.PutTask(context.Background(), newer); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.ListTasks(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].ID != "newer" {
		t.Fatalf("expected newest task, got %+v", tasks)
	}
}

func TestMemoryStoreSwitchAuthorizationExpiresOnRelease(t *testing.T) {
	s := NewMemoryStore("test")
	state, err := s.GetCluster(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.Primary = "mysql-1"
	if err := s.PutClusterIfEpoch(context.Background(), 0, state); err != nil {
		t.Fatal(err)
	}
	auth := model.SwitchAuthorization{OperationID: "op-1", FromEpoch: 0, ToEpoch: 1}
	_, release, err := s.AcquireSwitchLock(context.Background(), "controller-1", time.Second, auth)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.VerifySwitchOperation(context.Background(), auth); err != nil {
		t.Fatalf("active operation rejected: %v", err)
	}
	if err := s.VerifySwitchOperation(context.Background(), model.SwitchAuthorization{OperationID: "stale", FromEpoch: 0, ToEpoch: 1}); err == nil {
		t.Fatal("stale operation was authorized")
	}
	release()
	if err := s.VerifySwitchOperation(context.Background(), auth); err == nil {
		t.Fatal("released operation remained authorized")
	}
}

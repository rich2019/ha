package model

import "time"

type Role string

const (
	RoleUnknown Role = "unknown"
	RolePrimary Role = "primary"
	RoleReplica Role = "replica"
)

type NodeConfig struct {
	ID              string `json:"id"`
	Address         string `json:"address"`
	DSN             string `json:"dsn"`
	ReplicationUser string `json:"replication_user"`
	ReplicationPass string `json:"replication_password"`
	ExpectedRole    Role   `json:"expected_role"`
}

type ReplicaStatus struct {
	IOThreadRunning  bool   `json:"io_thread_running"`
	SQLThreadRunning bool   `json:"sql_thread_running"`
	SecondsBehind    *int64 `json:"seconds_behind"`
	LastIOError      string `json:"last_io_error,omitempty"`
	LastSQLError     string `json:"last_sql_error,omitempty"`
	ExecutedGTID     string `json:"executed_gtid,omitempty"`
	RetrievedGTID    string `json:"retrieved_gtid,omitempty"`
}

type NodeStatus struct {
	NodeID        string         `json:"node_id"`
	Address       string         `json:"address"`
	Reachable     bool           `json:"reachable"`
	Healthy       bool           `json:"healthy"`
	Role          Role           `json:"role"`
	ServerUUID    string         `json:"server_uuid,omitempty"`
	ReadOnly      bool           `json:"read_only"`
	SuperReadOnly bool           `json:"super_read_only"`
	Replica       *ReplicaStatus `json:"replica,omitempty"`
	Error         string         `json:"error,omitempty"`
	CheckedAt     time.Time      `json:"checked_at"`
	FailureCount  int            `json:"failure_count"`
}

type ClusterState struct {
	Name         string    `json:"name"`
	Primary      string    `json:"primary"`
	Epoch        uint64    `json:"epoch"`
	AutoFailover bool      `json:"auto_failover"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type SwitchTask struct {
	ID         string    `json:"id"`
	From       string    `json:"from,omitempty"`
	To         string    `json:"to"`
	Automatic  bool      `json:"automatic"`
	DryRun     bool      `json:"dry_run"`
	State      string    `json:"state"`
	Error      string    `json:"error,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

package model

import "time"

type Role string

const (
	RoleUnknown Role = "unknown"
	RolePrimary Role = "primary"
	RoleReplica Role = "replica"
)

type AgentNodeConfig struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	AgentURL string `json:"agent_url"`
}

type MySQLNodeConfig struct {
	ID              string
	Address         string
	DSN             string
	ReplicationUser string
	ReplicationPass string
}

type SwitchAuthorization struct {
	OperationID string `json:"operation_id"`
	FromEpoch   uint64 `json:"from_epoch"`
	ToEpoch     uint64 `json:"to_epoch"`
}

type ReplicaRequest struct {
	Authorization SwitchAuthorization `json:"authorization"`
	Source        AgentNodeConfig     `json:"source"`
}

type OperationRequest struct {
	Authorization SwitchAuthorization `json:"authorization"`
}

type WaitGTIDRequest struct {
	Authorization  SwitchAuthorization `json:"authorization"`
	GTID           string              `json:"gtid"`
	TimeoutSeconds int                 `json:"timeout_seconds"`
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

type AlertEvent struct {
	NodeID    string    `json:"node_id"`
	Severity  string    `json:"severity"`
	Title     string    `json:"title"`
	Message   string    `json:"message"`
	Resolved  bool      `json:"resolved"`
	CreatedAt time.Time `json:"created_at"`
}

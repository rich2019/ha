package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/ddchencm/ha/internal/model"
)

type Client struct {
	config model.MySQLNodeConfig
	db     *sql.DB
}

func NewClient(config model.MySQLNodeConfig) (*Client, error) {
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	return &Client{config: config, db: db}, nil
}

func (c *Client) Close() error { return c.db.Close() }

func (c *Client) Status(ctx context.Context) model.NodeStatus {
	status := model.NodeStatus{NodeID: c.config.ID, Address: c.config.Address, CheckedAt: time.Now().UTC(), Role: model.RoleUnknown}
	if err := c.db.PingContext(ctx); err != nil {
		status.Error = err.Error()
		return status
	}
	status.Reachable = true
	row := c.db.QueryRowContext(ctx, `SELECT @@GLOBAL.server_uuid, @@GLOBAL.read_only, @@GLOBAL.super_read_only`)
	if err := row.Scan(&status.ServerUUID, &status.ReadOnly, &status.SuperReadOnly); err != nil {
		status.Error = err.Error()
		return status
	}
	if !status.ReadOnly && !status.SuperReadOnly {
		status.Role = model.RolePrimary
		status.Healthy = true
		return status
	}
	replica, err := c.replicaStatus(ctx)
	if err != nil {
		status.Error = err.Error()
		status.Role = model.RoleReplica
		return status
	}
	status.Role = model.RoleReplica
	status.Replica = replica
	status.Healthy = replica.IOThreadRunning && replica.SQLThreadRunning && replica.SecondsBehind != nil
	return status
}

func (c *Client) ExecutedGTID(ctx context.Context) (string, error) {
	var set string
	err := c.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&set)
	return set, err
}

func (c *Client) WaitForGTID(ctx context.Context, set string, timeoutSeconds int) error {
	if set == "" {
		return errors.New("source GTID set is empty")
	}
	var timedOut sql.NullInt64
	if err := c.db.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, ?)", set, timeoutSeconds).Scan(&timedOut); err != nil {
		return err
	}
	if !timedOut.Valid || timedOut.Int64 != 0 {
		return errors.New("replica did not catch up to source GTID set")
	}
	return nil
}

func (c *Client) replicaStatus(ctx context.Context) (*model.ReplicaStatus, error) {
	rows, err := c.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		rows, err = c.db.QueryContext(ctx, "SHOW SLAVE STATUS")
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("replica status is empty")
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]any, len(columns))
	refs := make([]any, len(columns))
	for i := range values {
		refs[i] = &values[i]
	}
	if err := rows.Scan(refs...); err != nil {
		return nil, err
	}
	fields := make(map[string]string, len(columns))
	for i, column := range columns {
		if values[i] == nil {
			continue
		}
		switch value := values[i].(type) {
		case []byte:
			fields[column] = string(value)
		default:
			fields[column] = fmt.Sprint(value)
		}
	}
	result := &model.ReplicaStatus{
		IOThreadRunning:  fields["Replica_IO_Running"] == "Yes" || fields["Slave_IO_Running"] == "Yes",
		SQLThreadRunning: fields["Replica_SQL_Running"] == "Yes" || fields["Slave_SQL_Running"] == "Yes",
		LastIOError:      fields["Last_IO_Error"],
		LastSQLError:     fields["Last_SQL_Error"],
		ExecutedGTID:     first(fields["Executed_Gtid_Set"], fields["Exec_Source_Log_Pos"]),
		RetrievedGTID:    first(fields["Retrieved_Gtid_Set"], fields["Retrieved_GTID_Set"]),
	}
	lag := first(fields["Seconds_Behind_Source"], fields["Seconds_Behind_Master"])
	if lag != "" && lag != "NULL" {
		if parsed, err := strconv.ParseInt(lag, 10, 64); err == nil {
			result.SecondsBehind = &parsed
		}
	}
	return result, nil
}

func (c *Client) SetReadOnly(ctx context.Context, readOnly bool) error {
	value := "OFF"
	if readOnly {
		value = "ON"
	}
	if _, err := c.db.ExecContext(ctx, "SET GLOBAL super_read_only = "+value); err != nil {
		return err
	}
	_, err := c.db.ExecContext(ctx, "SET GLOBAL read_only = "+value)
	return err
}

func (c *Client) Promote(ctx context.Context) error {
	if _, err := c.db.ExecContext(ctx, "STOP REPLICA"); err != nil && !replicaChannelAbsent(err) {
		return err
	}
	if _, err := c.db.ExecContext(ctx, "RESET REPLICA ALL"); err != nil && !replicaChannelAbsent(err) {
		return err
	}
	return c.SetReadOnly(ctx, false)
}

func (c *Client) Demote(ctx context.Context) error { return c.SetReadOnly(ctx, true) }

func (c *Client) ReconfigureReplica(ctx context.Context, source model.AgentNodeConfig) error {
	if _, err := c.db.ExecContext(ctx, "STOP REPLICA"); err != nil && !replicaChannelAbsent(err) {
		return err
	}
	if _, err := c.db.ExecContext(ctx, "RESET REPLICA ALL"); err != nil && !replicaChannelAbsent(err) {
		return err
	}
	host, port := splitAddress(source.Address)
	if c.config.ReplicationUser == "" || c.config.ReplicationPass == "" {
		return errors.New("replication credentials are missing")
	}
	query := fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST = '%s', SOURCE_PORT = %d, SOURCE_USER = '%s', SOURCE_PASSWORD = '%s', SOURCE_AUTO_POSITION = 1", quoteSQL(host), port, quoteSQL(c.config.ReplicationUser), quoteSQL(c.config.ReplicationPass))
	if _, err := c.db.ExecContext(ctx, query); err != nil {
		return err
	}
	if _, err := c.db.ExecContext(ctx, "START REPLICA"); err != nil {
		return err
	}
	return c.SetReadOnly(ctx, true)
}

func replicaChannelAbsent(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not running") ||
		strings.Contains(message, "not configured as a replica") ||
		strings.Contains(message, "no replica defined for channel") ||
		strings.Contains(message, "no channels exist")
}

func quoteSQL(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `'`, `\'`)
}

func splitAddress(address string) (string, int) {
	parts := strings.Split(address, ":")
	if len(parts) == 2 {
		if parsed, err := strconv.Atoi(parts[1]); err == nil {
			return parts[0], parsed
		}
	}
	return address, 3306
}

func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

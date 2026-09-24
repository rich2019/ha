package config

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ddchencm/ha/internal/model"
)

type Config struct {
	HTTPAddr             string
	ClusterName          string
	ControllerID         string
	EtcdEndpoints        []string
	Nodes                []model.NodeConfig
	ProbeInterval        time.Duration
	FailureThreshold     int
	AlertWebhookURL      string
	ExecuteActions       bool
	AutoFailoverExecute  bool
	MaxReplicaLagSeconds int64
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:             env("HA_HTTP_ADDR", ":8080"),
		ClusterName:          env("HA_CLUSTER_NAME", "mysql-ha"),
		ControllerID:         env("HA_CONTROLLER_ID", hostname()),
		EtcdEndpoints:        split(env("HA_ETCD_ENDPOINTS", "http://127.0.0.1:2379")),
		ProbeInterval:        duration("HA_PROBE_INTERVAL", 5*time.Second),
		FailureThreshold:     integer("HA_FAILURE_THRESHOLD", 3),
		AlertWebhookURL:      os.Getenv("HA_ALERT_WEBHOOK_URL"),
		ExecuteActions:       boolean("HA_EXECUTE_ACTIONS", true),
		AutoFailoverExecute:  boolean("HA_AUTO_FAILOVER_EXECUTE", false),
		MaxReplicaLagSeconds: int64Value("HA_MAX_REPLICA_LAG_SECONDS", 30),
	}
	if raw := os.Getenv("HA_NODES_JSON"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.Nodes); err != nil {
			return Config{}, err
		}
	}
	return c, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func split(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func duration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func integer(key string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(key)); err == nil && value > 0 {
		return value
	}
	return fallback
}

func int64Value(key string, fallback int64) int64 {
	if value, err := strconv.ParseInt(os.Getenv(key), 10, 64); err == nil && value >= 0 {
		return value
	}
	return fallback
}

func boolean(key string, fallback bool) bool {
	if value, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		return value
	}
	return fallback
}

func hostname() string {
	if value, err := os.Hostname(); err == nil && value != "" {
		return value
	}
	return "ha-controller"
}

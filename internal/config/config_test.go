package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAgentsFromJSONFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.json")
	payload := `[{"id":"db-1","address":"10.0.0.1:3306","agent_url":"https://10.0.0.1:9443"}]`
	if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HA_AGENTS_JSON", "")
	t.Setenv("HA_AGENTS_FILE", path)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes) != 1 || cfg.Nodes[0].ID != "db-1" || cfg.Nodes[0].AgentURL != "https://10.0.0.1:9443" {
		t.Fatalf("unexpected agents config: %+v", cfg.Nodes)
	}
}

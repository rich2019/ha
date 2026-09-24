package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Manager struct {
	URL    string
	Client *http.Client
}

func NewManager(url string) *Manager {
	return &Manager{URL: url, Client: &http.Client{Timeout: 5 * time.Second}}
}

func (m *Manager) Send(ctx context.Context, severity, title, message string) error {
	if m.URL == "" {
		return nil
	}
	payload, err := json.Marshal(map[string]string{"severity": severity, "title": title, "message": message})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.URL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := m.Client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("alert webhook returned HTTP %d", response.StatusCode)
	}
	return nil
}

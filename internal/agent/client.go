package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ddchencm/ha/internal/model"
)

type Client struct {
	node model.AgentNodeConfig
	http *http.Client
}

type Node interface {
	Status(context.Context) model.NodeStatus
	ExecutedGTID(context.Context) (string, error)
	WaitForGTID(context.Context, model.SwitchAuthorization, string, int) error
	Demote(context.Context, model.SwitchAuthorization) error
	Promote(context.Context, model.SwitchAuthorization) error
	ReconfigureReplica(context.Context, model.SwitchAuthorization, model.AgentNodeConfig) error
	Close()
}

func NewClient(node model.AgentNodeConfig, caFile, certFile, keyFile string) (*Client, error) {
	if node.ID == "" || node.AgentURL == "" {
		return nil, errors.New("agent id and URL are required")
	}
	if !strings.HasPrefix(node.AgentURL, "https://") {
		return nil, fmt.Errorf("agent %s URL must use HTTPS", node.ID)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read agent CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("agent CA file contains no certificates")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load controller client certificate: %w", err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}}}
	return &Client{node: node, http: &http.Client{Transport: transport, Timeout: 75 * time.Second}}, nil
}

func (c *Client) Close() { c.http.CloseIdleConnections() }

func (c *Client) Status(ctx context.Context) model.NodeStatus {
	var status model.NodeStatus
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.get(probeCtx, "/api/v1/status", &status); err != nil {
		status = model.NodeStatus{NodeID: c.node.ID, Address: c.node.Address, Role: model.RoleUnknown, CheckedAt: time.Now().UTC(), Error: err.Error()}
		return status
	}
	status.NodeID = c.node.ID
	status.Address = c.node.Address
	return status
}

func (c *Client) ExecutedGTID(ctx context.Context) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var result struct {
		GTID string `json:"gtid"`
	}
	err := c.get(queryCtx, "/api/v1/gtid", &result)
	return result.GTID, err
}

func (c *Client) WaitForGTID(ctx context.Context, auth model.SwitchAuthorization, set string, timeoutSeconds int) error {
	return c.post(ctx, "/api/v1/wait-gtid", model.WaitGTIDRequest{Authorization: auth, GTID: set, TimeoutSeconds: timeoutSeconds}, nil)
}

func (c *Client) Demote(ctx context.Context, auth model.SwitchAuthorization) error {
	return c.post(ctx, "/api/v1/demote", model.OperationRequest{Authorization: auth}, nil)
}

func (c *Client) Promote(ctx context.Context, auth model.SwitchAuthorization) error {
	return c.post(ctx, "/api/v1/promote", model.OperationRequest{Authorization: auth}, nil)
}

func (c *Client) ReconfigureReplica(ctx context.Context, auth model.SwitchAuthorization, source model.AgentNodeConfig) error {
	return c.post(ctx, "/api/v1/replica", model.ReplicaRequest{Authorization: auth, Source: source}, nil)
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.node.AgentURL, "/")+path, nil)
	if err != nil {
		return err
	}
	return c.do(request, out)
}

func (c *Client) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.node.AgentURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	return c.do(request, out)
}

func (c *Client) do(request *http.Request, out any) error {
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		payload, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return fmt.Errorf("agent %s returned HTTP %d: %s", c.node.ID, response.StatusCode, strings.TrimSpace(string(payload)))
	}
	if out == nil {
		_, err = io.Copy(io.Discard, response.Body)
		return err
	}
	return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(out)
}

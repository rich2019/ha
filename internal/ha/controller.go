package ha

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ddchencm/ha/internal/alert"
	"github.com/ddchencm/ha/internal/config"
	"github.com/ddchencm/ha/internal/fence"
	"github.com/ddchencm/ha/internal/model"
	mysqlclient "github.com/ddchencm/ha/internal/mysql"
	"github.com/ddchencm/ha/internal/route"
	"github.com/ddchencm/ha/internal/store"
)

var (
	probeTotal   = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ha_probe_total", Help: "Total MySQL probes."}, []string{"node", "result"})
	probeHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ha_node_healthy", Help: "Whether a node is healthy."}, []string{"node"})
	lastSwitch   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "ha_last_switch_timestamp", Help: "Unix timestamp of the last completed switch."})
)

func init() { prometheus.MustRegister(probeTotal, probeHealthy, lastSwitch) }

type Controller struct {
	cfg     config.Config
	clients map[string]*mysqlclient.Client
	configs map[string]model.NodeConfig
	store   store.Store
	alerts  *alert.Manager
	router  route.Manager
	fencer  fence.Manager
	logger  *slog.Logger

	mu       sync.RWMutex
	statuses map[string]model.NodeStatus
	lastTask *model.SwitchTask
	tripping bool
}

func NewController(cfg config.Config, stateStore store.Store, logger *slog.Logger) (*Controller, error) {
	if len(cfg.Nodes) == 0 {
		return nil, errors.New("HA_NODES_JSON must contain at least one MySQL node")
	}
	c := &Controller{
		cfg: cfg, clients: make(map[string]*mysqlclient.Client), configs: make(map[string]model.NodeConfig),
		store: stateStore, alerts: alert.NewManager(cfg.AlertWebhookURL), router: route.NoopManager{},
		fencer: fence.NoopManager{}, logger: logger, statuses: make(map[string]model.NodeStatus),
	}
	for _, node := range cfg.Nodes {
		if node.ID == "" || node.DSN == "" {
			return nil, fmt.Errorf("node %q must define id and dsn", node.ID)
		}
		client, err := mysqlclient.NewClient(node)
		if err != nil {
			return nil, fmt.Errorf("create client %s: %w", node.ID, err)
		}
		c.clients[node.ID] = client
		c.configs[node.ID] = node
	}
	return c, nil
}

func (c *Controller) Close() error {
	for _, client := range c.clients {
		_ = client.Close()
	}
	return c.store.Close()
}

func (c *Controller) Run(ctx context.Context) error {
	return c.store.RunLeaderElection(ctx, c.cfg.ControllerID, c.runAsLeader)
}

func (c *Controller) runAsLeader(ctx context.Context) {
	c.probe(ctx)
	ticker := time.NewTicker(c.cfg.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probe(ctx)
		}
	}
}

func (c *Controller) probe(ctx context.Context) {
	c.mu.Lock()
	for id, client := range c.clients {
		status := client.Status(ctx)
		previous := c.statuses[id]
		if status.Healthy {
			status.FailureCount = 0
			probeTotal.WithLabelValues(id, "healthy").Inc()
			probeHealthy.WithLabelValues(id).Set(1)
		} else {
			status.FailureCount = previous.FailureCount + 1
			probeTotal.WithLabelValues(id, "unhealthy").Inc()
			probeHealthy.WithLabelValues(id).Set(0)
		}
		c.statuses[id] = status
	}
	c.mu.Unlock()

	state, err := c.store.GetCluster(ctx)
	if err != nil {
		c.logger.Error("read cluster state", "error", err)
		return
	}
	if state.Primary == "" {
		state.Primary = c.detectPrimary()
		state.UpdatedAt = time.Now().UTC()
		_ = c.store.PutCluster(ctx, state)
	}
	if state.AutoFailover && state.Primary != "" {
		c.tryAutomaticFailover(ctx, state.Primary)
	}
}

func (c *Controller) detectPrimary() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for id, status := range c.statuses {
		if status.Healthy && status.Role == model.RolePrimary {
			return id
		}
	}
	return ""
}

func (c *Controller) tryAutomaticFailover(ctx context.Context, primary string) {
	c.mu.RLock()
	status, ok := c.statuses[primary]
	c.mu.RUnlock()
	if !ok || status.Healthy || status.FailureCount < c.cfg.FailureThreshold || c.isTripping() {
		return
	}
	target, err := c.chooseTarget(primary)
	if err != nil {
		c.logger.Error("choose automatic failover target", "error", err)
		return
	}
	_, _ = c.Switch(ctx, target, true)
}

func (c *Controller) isTripping() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tripping
}

func (c *Controller) Status() (model.ClusterState, map[string]model.NodeStatus, *model.SwitchTask, error) {
	state, err := c.store.GetCluster(context.Background())
	c.mu.RLock()
	defer c.mu.RUnlock()
	copyStatuses := make(map[string]model.NodeStatus, len(c.statuses))
	for id, status := range c.statuses {
		copyStatuses[id] = status
	}
	return state, copyStatuses, c.lastTask, err
}

func (c *Controller) SetAutoFailover(ctx context.Context, enabled bool) error {
	state, err := c.store.GetCluster(ctx)
	if err != nil {
		return err
	}
	state.AutoFailover = enabled
	state.UpdatedAt = time.Now().UTC()
	return c.store.PutCluster(ctx, state)
}

func (c *Controller) Switch(ctx context.Context, target string, automatic bool) (*model.SwitchTask, error) {
	state, err := c.store.GetCluster(ctx)
	if err != nil {
		return nil, err
	}
	if target == "" {
		target, err = c.chooseTarget(state.Primary)
		if err != nil {
			return nil, err
		}
	}
	if _, ok := c.clients[target]; !ok {
		return nil, fmt.Errorf("unknown target node %q", target)
	}
	if target == state.Primary {
		return nil, errors.New("target is already the current primary")
	}
	release, err := c.store.AcquireLock(ctx, c.cfg.ControllerID, 30*time.Second)
	if err != nil {
		return nil, err
	}
	defer release()

	dryRun := !c.cfg.ExecuteActions || (automatic && !c.cfg.AutoFailoverExecute)
	task := &model.SwitchTask{ID: fmt.Sprintf("switch-%d", time.Now().UnixNano()), From: state.Primary, To: target, Automatic: automatic, DryRun: dryRun, State: "running", StartedAt: time.Now().UTC()}
	c.mu.Lock()
	c.tripping = true
	c.lastTask = task
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.tripping = false
		c.mu.Unlock()
	}()
	if dryRun {
		task.State = "dry-run"
		task.FinishedAt = time.Now().UTC()
		_ = c.alerts.Send(ctx, "warning", "MySQL HA automatic failover dry-run", fmt.Sprintf("candidate=%s current_primary=%s", target, state.Primary))
		return task, nil
	}

	if state.Primary != "" {
		if err := c.fencer.Fence(ctx, state.Primary); err != nil {
			return c.finishTask(task, err)
		}
		if client := c.clients[state.Primary]; client != nil {
			if err := client.Demote(ctx); err != nil {
				return c.finishTask(task, fmt.Errorf("demote old primary: %w", err))
			}
		}
	}
	if err := c.clients[target].Promote(ctx); err != nil {
		return c.finishTask(task, fmt.Errorf("promote %s: %w", target, err))
	}
	if err := c.router.SetPrimary(ctx, target); err != nil {
		return c.finishTask(task, fmt.Errorf("update route: %w", err))
	}
	for id, client := range c.clients {
		if id == target {
			continue
		}
		if err := client.ReconfigureReplica(ctx, c.configs[target]); err != nil {
			return c.finishTask(task, fmt.Errorf("reconfigure replica %s: %w", id, err))
		}
	}
	state.Primary = target
	state.Epoch++
	state.UpdatedAt = time.Now().UTC()
	if err := c.store.PutCluster(ctx, state); err != nil {
		return c.finishTask(task, fmt.Errorf("persist cluster state: %w", err))
	}
	lastSwitch.Set(float64(time.Now().Unix()))
	_ = c.alerts.Send(ctx, "info", "MySQL HA switch completed", fmt.Sprintf("new_primary=%s epoch=%d", target, state.Epoch))
	task.State = "completed"
	task.FinishedAt = time.Now().UTC()
	return task, nil
}

func (c *Controller) finishTask(task *model.SwitchTask, err error) (*model.SwitchTask, error) {
	task.State = "failed"
	task.Error = err.Error()
	task.FinishedAt = time.Now().UTC()
	_ = c.alerts.Send(context.Background(), "critical", "MySQL HA switch failed", err.Error())
	return task, err
}

func (c *Controller) chooseTarget(exclude string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	type candidate struct {
		id  string
		lag int64
	}
	candidates := make([]candidate, 0)
	for id, status := range c.statuses {
		if id == exclude || !status.Healthy || status.Role != model.RoleReplica {
			continue
		}
		lag := int64(0)
		if status.Replica != nil && status.Replica.SecondsBehind != nil {
			lag = *status.Replica.SecondsBehind
		}
		if lag <= c.cfg.MaxReplicaLagSeconds {
			candidates = append(candidates, candidate{id: id, lag: lag})
		}
	}
	if len(candidates) == 0 {
		return "", errors.New("no healthy replica is eligible for promotion")
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].lag == candidates[j].lag {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].lag < candidates[j].lag
	})
	return candidates[0].id, nil
}

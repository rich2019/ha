package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/ddchencm/ha/internal/model"
)

type Store interface {
	GetCluster(context.Context) (model.ClusterState, error)
	PutCluster(context.Context, model.ClusterState) error
	PutClusterIfEpoch(context.Context, uint64, model.ClusterState) error
	AcquireLock(context.Context, string, time.Duration) (context.Context, func(), error)
	AcquireSwitchLock(context.Context, string, time.Duration, model.SwitchAuthorization) (context.Context, func(), error)
	VerifySwitchOperation(context.Context, model.SwitchAuthorization) error
	RunLeaderElection(context.Context, string, func(context.Context)) error
	PutTask(context.Context, model.SwitchTask) error
	ListTasks(context.Context, int) ([]model.SwitchTask, error)
	Close() error
}

type MemoryStore struct {
	mu        sync.Mutex
	cluster   model.ClusterState
	locked    bool
	tasks     map[string]model.SwitchTask
	operation *model.SwitchAuthorization
}

func NewMemoryStore(name string) Store {
	return &MemoryStore{cluster: model.ClusterState{Name: name}, tasks: make(map[string]model.SwitchTask)}
}

func (s *MemoryStore) GetCluster(context.Context) (model.ClusterState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cluster, nil
}

func (s *MemoryStore) PutCluster(_ context.Context, state model.ClusterState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cluster = state
	return nil
}

func (s *MemoryStore) PutClusterIfEpoch(_ context.Context, expected uint64, state model.ClusterState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cluster.Epoch != expected {
		return errors.New("cluster epoch changed")
	}
	s.cluster = state
	return nil
}

func (s *MemoryStore) AcquireLock(ctx context.Context, _ string, _ time.Duration) (context.Context, func(), error) {
	s.mu.Lock()
	if s.locked {
		s.mu.Unlock()
		return nil, nil, errors.New("switch lock is held")
	}
	s.locked = true
	s.mu.Unlock()
	return ctx, func() {
		s.mu.Lock()
		s.locked = false
		s.mu.Unlock()
	}, nil
}

func (s *MemoryStore) AcquireSwitchLock(ctx context.Context, holder string, ttl time.Duration, auth model.SwitchAuthorization) (context.Context, func(), error) {
	lockCtx, release, err := s.AcquireLock(ctx, holder, ttl)
	if err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	copy := auth
	s.operation = &copy
	s.mu.Unlock()
	return lockCtx, func() {
		s.mu.Lock()
		s.operation = nil
		s.mu.Unlock()
		release()
	}, nil
}

func (s *MemoryStore) VerifySwitchOperation(_ context.Context, auth model.SwitchAuthorization) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.operation == nil || *s.operation != auth {
		return errors.New("switch operation is not authorized")
	}
	if s.cluster.Epoch != auth.FromEpoch && s.cluster.Epoch != auth.ToEpoch {
		return errors.New("switch operation epoch is stale")
	}
	return nil
}

func (s *MemoryStore) PutTask(_ context.Context, task model.SwitchTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID] = task
	return nil
}

func (s *MemoryStore) ListTasks(_ context.Context, limit int) ([]model.SwitchTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tasks := make([]model.SwitchTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		tasks = append(tasks, task)
	}
	sortTasks(tasks)
	if limit > 0 && len(tasks) > limit {
		tasks = tasks[:limit]
	}
	return tasks, nil
}

func (*MemoryStore) Close() error { return nil }

func (s *MemoryStore) RunLeaderElection(ctx context.Context, _ string, onLeader func(context.Context)) error {
	onLeader(ctx)
	return ctx.Err()
}

type EtcdStore struct {
	client *clientv3.Client
	prefix string
}

func NewEtcdStore(endpoints []string, clusterName string) (Store, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("at least one Etcd endpoint is required")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, endpoint := range endpoints {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if _, lastErr = client.Status(ctx, endpoint); lastErr == nil {
			cancel()
			return &EtcdStore{client: client, prefix: "/ha/" + clusterName}, nil
		}
		cancel()
	}
	if lastErr != nil {
		_ = client.Close()
		return nil, fmt.Errorf("no Etcd endpoint is reachable: %w", lastErr)
	}
	_ = client.Close()
	return nil, errors.New("no Etcd endpoint is reachable")
}

func (s *EtcdStore) GetCluster(ctx context.Context) (model.ClusterState, error) {
	response, err := s.client.Get(ctx, s.prefix+"/cluster")
	if err != nil {
		return model.ClusterState{}, err
	}
	if len(response.Kvs) == 0 {
		return model.ClusterState{Name: s.prefix[4:]}, nil
	}
	var state model.ClusterState
	if err := json.Unmarshal(response.Kvs[0].Value, &state); err != nil {
		return model.ClusterState{}, err
	}
	return state, nil
}

func (s *EtcdStore) PutCluster(ctx context.Context, state model.ClusterState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, s.prefix+"/cluster", string(payload))
	return err
}

func (s *EtcdStore) PutClusterIfEpoch(ctx context.Context, expected uint64, state model.ClusterState) error {
	key := s.prefix + "/cluster"
	response, err := s.client.Get(ctx, key)
	if err != nil {
		return err
	}
	compare := clientv3.Compare(clientv3.Version(key), "=", 0)
	if len(response.Kvs) > 0 {
		var current model.ClusterState
		if err := json.Unmarshal(response.Kvs[0].Value, &current); err != nil {
			return err
		}
		if current.Epoch != expected {
			return errors.New("cluster epoch changed")
		}
		compare = clientv3.Compare(clientv3.ModRevision(key), "=", response.Kvs[0].ModRevision)
	} else if expected != 0 {
		return errors.New("cluster epoch changed")
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	result, err := s.client.Txn(ctx).If(compare).Then(clientv3.OpPut(key, string(payload))).Commit()
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errors.New("cluster state changed concurrently")
	}
	return nil
}

func (s *EtcdStore) AcquireLock(ctx context.Context, holder string, ttl time.Duration) (context.Context, func(), error) {
	lockCtx, release, _, err := s.acquireEtcdLock(ctx, holder, ttl)
	return lockCtx, release, err
}

func (s *EtcdStore) AcquireSwitchLock(ctx context.Context, holder string, ttl time.Duration, auth model.SwitchAuthorization) (context.Context, func(), error) {
	state, err := s.GetCluster(ctx)
	if err != nil {
		return nil, nil, err
	}
	if state.Epoch != auth.FromEpoch || auth.ToEpoch != auth.FromEpoch+1 || auth.OperationID == "" {
		return nil, nil, errors.New("invalid switch authorization epoch")
	}
	lockCtx, release, session, err := s.acquireEtcdLock(ctx, holder, ttl)
	if err != nil {
		return nil, nil, err
	}
	payload, err := json.Marshal(auth)
	if err == nil {
		_, err = s.client.Put(lockCtx, s.prefix+"/switch-operation", string(payload), clientv3.WithLease(session.Lease()))
	}
	if err != nil {
		release()
		return nil, nil, err
	}
	return lockCtx, func() {
		_, _ = s.client.Delete(context.Background(), s.prefix+"/switch-operation")
		release()
	}, nil
}

func (s *EtcdStore) VerifySwitchOperation(ctx context.Context, auth model.SwitchAuthorization) error {
	if auth.OperationID == "" || auth.ToEpoch != auth.FromEpoch+1 {
		return errors.New("invalid switch authorization")
	}
	state, err := s.GetCluster(ctx)
	if err != nil {
		return err
	}
	if state.Epoch != auth.FromEpoch && state.Epoch != auth.ToEpoch {
		return errors.New("switch operation epoch is stale")
	}
	response, err := s.client.Get(ctx, s.prefix+"/switch-operation")
	if err != nil {
		return err
	}
	if len(response.Kvs) != 1 {
		return errors.New("no active switch operation")
	}
	var active model.SwitchAuthorization
	if err := json.Unmarshal(response.Kvs[0].Value, &active); err != nil {
		return err
	}
	if active != auth {
		return errors.New("switch operation authorization mismatch")
	}
	return nil
}

func (s *EtcdStore) acquireEtcdLock(ctx context.Context, holder string, ttl time.Duration) (context.Context, func(), *concurrency.Session, error) {
	ttlSeconds := int(ttl.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}
	session, err := concurrency.NewSession(s.client, concurrency.WithTTL(ttlSeconds), concurrency.WithContext(ctx))
	if err != nil {
		return nil, nil, nil, err
	}
	mutex := concurrency.NewMutex(session, s.prefix+"/switch-lock/")
	if err := mutex.Lock(ctx); err != nil {
		_ = session.Close()
		return nil, nil, nil, err
	}
	_ = holder
	lockCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-session.Done():
			cancel()
		case <-lockCtx.Done():
		}
	}()
	var once sync.Once
	return lockCtx, func() {
		once.Do(func() {
			cancel()
			_ = mutex.Unlock(context.Background())
			_ = session.Close()
		})
	}, session, nil
}

func (s *EtcdStore) PutTask(ctx context.Context, task model.SwitchTask) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, s.prefix+"/tasks/"+task.ID, string(payload))
	return err
}

func (s *EtcdStore) ListTasks(ctx context.Context, limit int) ([]model.SwitchTask, error) {
	response, err := s.client.Get(ctx, s.prefix+"/tasks/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend))
	if err != nil {
		return nil, err
	}
	tasks := make([]model.SwitchTask, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		var task model.SwitchTask
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
		if limit > 0 && len(tasks) >= limit {
			break
		}
	}
	return tasks, nil
}

func (s *EtcdStore) RunLeaderElection(ctx context.Context, id string, onLeader func(context.Context)) error {
	for ctx.Err() == nil {
		session, err := concurrency.NewSession(s.client, concurrency.WithTTL(10), concurrency.WithContext(ctx))
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(time.Second)
			continue
		}
		election := concurrency.NewElection(session, s.prefix+"/leader")
		if err := election.Campaign(ctx, id); err != nil {
			_ = session.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(time.Second)
			continue
		}
		leaderCtx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-session.Done():
				cancel()
			case <-ctx.Done():
				cancel()
			}
		}()
		onLeader(leaderCtx)
		cancel()
		_ = election.Resign(context.Background())
		_ = session.Close()
	}
	return ctx.Err()
}

func (s *EtcdStore) Close() error { return s.client.Close() }

func sortTasks(tasks []model.SwitchTask) {
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].StartedAt.After(tasks[j].StartedAt) })
}

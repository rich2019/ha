package store

import (
	"context"
	"encoding/json"
	"errors"
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
	RunLeaderElection(context.Context, string, func(context.Context)) error
	PutTask(context.Context, model.SwitchTask) error
	ListTasks(context.Context, int) ([]model.SwitchTask, error)
	Close() error
}

type MemoryStore struct {
	mu      sync.Mutex
	cluster model.ClusterState
	locked  bool
	tasks   map[string]model.SwitchTask
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
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Status(ctx, endpoints[0]); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &EtcdStore{client: client, prefix: "/ha/" + clusterName}, nil
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
	ttlSeconds := int(ttl.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}
	session, err := concurrency.NewSession(s.client, concurrency.WithTTL(ttlSeconds), concurrency.WithContext(ctx))
	if err != nil {
		return nil, nil, err
	}
	mutex := concurrency.NewMutex(session, s.prefix+"/switch-lock/")
	if err := mutex.Lock(ctx); err != nil {
		_ = session.Close()
		return nil, nil, err
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
	}, nil
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

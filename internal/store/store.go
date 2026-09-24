package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/ddchencm/ha/internal/model"
)

type Store interface {
	GetCluster(context.Context) (model.ClusterState, error)
	PutCluster(context.Context, model.ClusterState) error
	AcquireLock(context.Context, string, time.Duration) (func(), error)
	RunLeaderElection(context.Context, string, func(context.Context)) error
	Close() error
}

type MemoryStore struct {
	mu      sync.Mutex
	cluster model.ClusterState
	locked  bool
}

func NewMemoryStore(name string) Store { return &MemoryStore{cluster: model.ClusterState{Name: name}} }

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

func (s *MemoryStore) AcquireLock(_ context.Context, _ string, _ time.Duration) (func(), error) {
	s.mu.Lock()
	if s.locked {
		s.mu.Unlock()
		return nil, errors.New("switch lock is held")
	}
	s.locked = true
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		s.locked = false
		s.mu.Unlock()
	}, nil
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

func (s *EtcdStore) AcquireLock(ctx context.Context, holder string, ttl time.Duration) (func(), error) {
	ttlSeconds := int(ttl.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}
	session, err := concurrency.NewSession(s.client, concurrency.WithTTL(ttlSeconds), concurrency.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	mutex := concurrency.NewMutex(session, s.prefix+"/switch-lock/")
	if err := mutex.Lock(ctx); err != nil {
		_ = session.Close()
		return nil, err
	}
	_ = holder
	return func() {
		_ = mutex.Unlock(context.Background())
		_ = session.Close()
	}, nil
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

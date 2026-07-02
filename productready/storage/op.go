package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xkeyideal/mraft/productready/storage/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

func (s *Storage) clusterResources(clusterId uint64) (*dragonboat.NodeHost, *client.Session, *store.Store, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.nh == nil {
		return nil, nil, nil, errors.New("raft storage not ready")
	}

	cs, ok := s.csMap[clusterId]
	if !ok {
		return nil, nil, nil, fmt.Errorf("cluster %d session not found", clusterId)
	}

	st, ok := s.smMap[clusterId]
	if !ok {
		return nil, nil, nil, fmt.Errorf("cluster %d store not found", clusterId)
	}

	return s.nh, cs, st, nil
}

func (s *Storage) nodeHost() (*dragonboat.NodeHost, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.nh == nil {
		return nil, errors.New("raft storage not ready")
	}
	return s.nh, nil
}

func (s *Storage) Get(ctx context.Context, cf string, hashKey string, linearizable bool, key []byte) ([]byte, error) {
	clusterId := s.getClusterId(hashKey)
	cmd := NewGetCommand(cf, key)

	nh, cs, st, err := s.clusterResources(clusterId)
	if err != nil {
		return nil, err
	}

	if linearizable {
		err = cmd.RaftInvoke(ctx, nh, clusterId, cs)
	} else {
		err = cmd.LocalInvoke(st)
	}
	return cmd.GetResult(), err
}

func (s *Storage) Put(ctx context.Context, cf string, hashKey string, key, val []byte) error {
	cmd := NewPutCommand(cf, key, val)
	clusterId := s.getClusterId(hashKey)

	nh, cs, _, err := s.clusterResources(clusterId)
	if err != nil {
		return err
	}

	return cmd.RaftInvoke(ctx, nh, clusterId, cs)
}

func (s *Storage) Del(ctx context.Context, cf string, hashKey string, key []byte) error {
	cmd := NewDelCommand(cf, key)
	clusterId := s.getClusterId(hashKey)

	nh, cs, _, err := s.clusterResources(clusterId)
	if err != nil {
		return err
	}

	return cmd.RaftInvoke(ctx, nh, clusterId, cs)
}

func (s *Storage) AddRaftNode(nodeId uint64, target string) error {
	nh, err := s.nodeHost()
	if err != nil {
		return err
	}

	var errs []error
	for _, clusterId := range s.cfg.ClusterIds {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ms, err := nh.SyncGetClusterMembership(ctx, uint64(clusterId))
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("cluster %d get membership: %w", clusterId, err))
			continue
		}

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		err = nh.SyncRequestAddNode(ctx, uint64(clusterId), nodeId, target, ms.ConfigChangeID)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("cluster %d add node: %w", clusterId, err))
			continue
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("partial failure: %v", errs)
	}
	return nil
}

func (s *Storage) AddRaftObserver(nodeId uint64, addr string) error {
	nh, err := s.nodeHost()
	if err != nil {
		return err
	}

	var errs []error
	for _, clusterId := range s.cfg.ClusterIds {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ms, err := nh.SyncGetClusterMembership(ctx, uint64(clusterId))
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("cluster %d get membership: %w", clusterId, err))
			continue
		}

		ctx, cancel = context.WithTimeout(context.Background(), 4*time.Second)
		err = nh.SyncRequestAddObserver(ctx, uint64(clusterId), nodeId, addr, ms.ConfigChangeID)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("cluster %d add observer: %w", clusterId, err))
			continue
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("partial failure: %v", errs)
	}
	return nil
}

func (s *Storage) RemoveRaftNode(nodeId uint64) error {
	nh, err := s.nodeHost()
	if err != nil {
		return err
	}

	var errs []error
	for _, clusterId := range s.cfg.ClusterIds {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ms, err := nh.SyncGetClusterMembership(ctx, uint64(clusterId))
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("cluster %d get membership: %w", clusterId, err))
			continue
		}

		ctx, cancel = context.WithTimeout(context.Background(), 4*time.Second)
		err = nh.SyncRequestDeleteNode(ctx, uint64(clusterId), nodeId, ms.ConfigChangeID)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("cluster %d remove node: %w", clusterId, err))
			continue
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("partial failure: %v", errs)
	}
	return nil
}

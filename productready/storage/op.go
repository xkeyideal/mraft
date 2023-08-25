package storage

import (
	"context"
	"time"
)

func (s *Storage) Get(ctx context.Context, cf string, hashKey string, linearizable bool, key []byte) ([]byte, error) {
	var (
		clusterId = s.getClusterId(hashKey)
		cmd       = NewGetCommand(cf, key)
		err       error
	)

	if linearizable {
		err = cmd.RaftInvoke(ctx, s.nh, clusterId, s.csMap[clusterId])
	} else {
		err = cmd.LocalInvoke(s.smMap[clusterId])
	}
	return cmd.GetResult(), err
}

func (s *Storage) Put(ctx context.Context, cf string, hashKey string, key, val []byte) error {
	cmd := NewPutCommand(cf, key, val)
	clusterId := s.getClusterId(hashKey)
	return cmd.RaftInvoke(ctx, s.nh, clusterId, s.csMap[clusterId])
}

func (s *Storage) Del(ctx context.Context, cf string, hashKey string, key []byte) error {
	cmd := NewDelCommand(cf, key)
	clusterId := s.getClusterId(hashKey)
	return cmd.RaftInvoke(ctx, s.nh, clusterId, s.csMap[clusterId])
}

func (s *Storage) AddRaftNode(nodeId uint64, target string) error {
	for _, clusterId := range s.cfg.ClusterIds {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ms, err := s.nh.SyncGetClusterMembership(ctx, uint64(clusterId))
		cancel()
		if err != nil {
			return err
		}

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		err = s.nh.SyncRequestAddNode(ctx, uint64(clusterId), nodeId, target, ms.ConfigChangeID)
		cancel()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) AddRaftObserver(nodeId uint64, addr string) error {
	for _, clusterId := range s.cfg.ClusterIds {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ms, err := s.nh.SyncGetClusterMembership(ctx, uint64(clusterId))
		cancel()
		if err != nil {
			return err
		}

		ctx, cancel = context.WithTimeout(context.Background(), 4*time.Second)
		err = s.nh.SyncRequestAddObserver(ctx, uint64(clusterId), nodeId, addr, ms.ConfigChangeID)
		cancel()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) RemoveRaftNode(nodeId uint64) error {
	for _, clusterId := range s.cfg.ClusterIds {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ms, err := s.nh.SyncGetClusterMembership(ctx, uint64(clusterId))
		cancel()
		if err != nil {
			return err
		}

		ctx, cancel = context.WithTimeout(context.Background(), 4*time.Second)
		err = s.nh.SyncRequestDeleteNode(ctx, uint64(clusterId), nodeId, ms.ConfigChangeID)
		cancel()
		if err != nil {
			return err
		}
	}

	return nil
}

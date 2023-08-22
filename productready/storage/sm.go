package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"log"

	"github.com/xkeyideal/mraft/productready/storage/store"

	"github.com/cockroachdb/pebble"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

var (
	indexKeyPrefix = []byte("__RAFT_APPLIED_INDEX__")
	nodeReadyKey   = []byte("__RAFT_NODE_READY__")
	nodeReadyVal   = []byte("^_^")

	moveToErr       = errors.New("MoveTo Jump Exceed")
	sessionNotFound = errors.New("Raft clusterId session not found")
	storeNotFound   = errors.New("Raft clusterId store not found")
)

type StateMachine struct {
	raftAddr  string
	target    string
	ClusterID uint64
	NodeID    uint64
	store     *store.Store

	// db里存储revision的key
	indexKey []byte
}

func newStateMachine(raftAddr, target string, clusterid uint64, nodeId uint64, s *store.Store) *StateMachine {
	// 生成存储revision的key
	smIndexKey := make([]byte, len(indexKeyPrefix)+8)
	copy(smIndexKey, indexKeyPrefix)
	binary.BigEndian.PutUint64(smIndexKey[len(indexKeyPrefix):], clusterid)

	return &StateMachine{
		raftAddr:  raftAddr,
		target:    target,
		ClusterID: clusterid,
		NodeID:    nodeId,
		indexKey:  smIndexKey,
		store:     s,
	}
}

func (r *StateMachine) Open(stopChan <-chan struct{}) (uint64, error) {
	select {
	case <-stopChan:
		return 0, sm.ErrOpenStopped
	default:
		val, err := r.store.GetBytes(r.indexKey)
		if err != nil {
			return 0, err
		}

		// 系统初次启动时，全局revision应该是不存在的，db里查不到，此时返回0
		if len(val) == 0 {
			return 0, nil
		}

		return binary.BigEndian.Uint64(val), nil
	}
}

func (r *StateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if r.store.Closed() {
		return entries, nil
	}

	resultEntries := make([]sm.Entry, 0, len(entries))

	//  将raft的日志转换为db要执行的命令
	for _, e := range entries {
		r, err := r.processEntry(e)
		if err != nil {
			return nil, err
		}

		resultEntries = append(resultEntries, r)
	}

	idx := entries[len(entries)-1].Index
	idxByte := make([]byte, 8)
	binary.BigEndian.PutUint64(idxByte, idx)

	batch := r.store.Batch()
	defer batch.Close()

	// 更新revision的值
	batch.Set(r.indexKey, idxByte, r.store.GetWo())
	if err := r.store.Write(batch); err != nil {
		return nil, err
	}

	return resultEntries, nil
}

func (r *StateMachine) processEntry(e sm.Entry) (sm.Entry, error) {
	cmd, err := DecodeCmd(e.Cmd)
	if err != nil {
		return e, err
	}

	opts := &WriteOptions{
		Revision: e.Index,
	}

	if err := cmd.LocalInvoke(r.store, opts); err != nil {
		return e, err
	}

	resp := cmd.GetResp()
	e.Result = sm.Result{Value: uint64(len(e.Cmd)), Data: resp}

	return e, nil
}

func (r *StateMachine) Lookup(query interface{}) (interface{}, error) {
	if r.store.Closed() {
		return nil, pebble.ErrClosed
	}

	cmd, err := DecodeCmd(query.([]byte))
	if err != nil {
		return nil, err
	}

	if err := cmd.LocalInvoke(r.store); err != nil {
		return nil, err
	}

	return cmd.GetResp(), nil
}

func (r *StateMachine) Sync() error {
	return nil
}

type stateMachineStoreCtx struct {
	snapshot *pebble.Snapshot
}

func (r *StateMachine) PrepareSnapshot() (interface{}, error) {
	if r.store.Closed() {
		return nil, pebble.ErrClosed
	}

	return &stateMachineStoreCtx{
		snapshot: r.store.GetSnapshot(),
	}, nil
}

func (r *StateMachine) SaveSnapshot(snapshot interface{}, writer io.Writer, stopChan <-chan struct{}) error {
	if r.store.Closed() {
		return pebble.ErrClosed
	}

	log.Println("SaveSnapshot", r.target, r.raftAddr, r.NodeID, r.ClusterID)
	ctxData := snapshot.(*stateMachineStoreCtx)

	ss := ctxData.snapshot
	defer ss.Close()

	return r.store.SaveSnapshotToWriter(r.target, r.raftAddr, ss, writer, stopChan)
}

func (r *StateMachine) RecoverFromSnapshot(reader io.Reader, stopChan <-chan struct{}) error {
	if r.store.Closed() {
		return pebble.ErrClosed
	}

	log.Println("RecoverFromSnapshot", r.target, r.raftAddr, r.NodeID, r.ClusterID)
	return r.store.LoadSnapShotFromReader(r.target, r.raftAddr, reader, stopChan)
}

func (r *StateMachine) Close() error {
	if r.store.Closed() {
		return nil
	}

	return r.store.Close()
}

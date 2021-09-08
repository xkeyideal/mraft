package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/tecbot/gorocksdb"
)

var indexKeyCf = "__index_default_cf__"
var indexKeyPrefix = []byte("disk_kv_applied_index")

type RocksDBStateMachine struct {
	ClusterID      uint64
	NodeID         uint64
	store          *Store
	indexKeyPrefix []byte
}

func newRocksDBStateMachine(clusterId uint64, nodeId uint64, s *Store) (*RocksDBStateMachine, error) {
	smIndexKey := make([]byte, len(indexKeyPrefix)+16)
	copy(smIndexKey, indexKeyPrefix)
	binary.BigEndian.PutUint64(smIndexKey[len(indexKeyPrefix):], clusterId)
	binary.BigEndian.PutUint64(smIndexKey[len(indexKeyPrefix)+8:], nodeId)
	return &RocksDBStateMachine{ClusterID: clusterId, NodeID: nodeId, indexKeyPrefix: smIndexKey, store: s}, nil
}

func (r *RocksDBStateMachine) Open(stopChan <-chan struct{}) (uint64, error) {
	select {
	case <-stopChan:
		return 0, sm.ErrOpenStopped
	default:
		data, err := r.store.GetUint64(indexKeyCf, r.indexKeyPrefix)
		return data, err
	}
}

func (r *RocksDBStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	resultEntry := []sm.Entry{}
	//将raft的日志转换为rocksdb要执行的命令

	for _, e := range entries {
		r, err := r.processEntry(e)
		if err != nil {
			return nil, err
		}
		resultEntry = append(resultEntry, r)
	}

	idx := entries[len(entries)-1].Index
	idxByte := make([]byte, 8)
	binary.BigEndian.PutUint64(idxByte, idx)
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(r.indexKeyPrefix, idxByte)

	if err := r.store.Write(batch); err != nil {
		return nil, err
	}

	return resultEntry, nil
}

func (r *RocksDBStateMachine) processEntry(e sm.Entry) (sm.Entry, error) {
	cmd := DecodeCmd(e.Cmd)
	if cmd == nil {
		return sm.Entry{}, errors.New("error command!")
	}

	if err := cmd.LocalInvoke(r.store); err != nil {
		return e, err
	}

	resp := cmd.GetResp()
	if len(resp) > 0 {
		e.Result = sm.Result{Value: 1, Data: resp}
	}

	return e, nil
}

func (r *RocksDBStateMachine) Lookup(query interface{}) (interface{}, error) {
	cmd := DecodeCmd(query.([]byte))
	if cmd == nil {
		return nil, errors.New("error command!")
	}

	if err := cmd.LocalInvoke(r.store); err != nil {
		return nil, err
	}

	return cmd.GetResp(), nil
}

func (r *RocksDBStateMachine) Sync() error {
	return nil
}

func (r *RocksDBStateMachine) PrepareSnapshot() (interface{}, error) {
	return r.store.NewSnapshotDir()
}

func (r *RocksDBStateMachine) SaveSnapshot(snapshot interface{}, writer io.Writer, stopChan <-chan struct{}) error {
	path := snapshot.(string)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	return r.store.SaveSnapShotToWriter(path, writer, stopChan)
}

func (r *RocksDBStateMachine) RecoverFromSnapshot(reader io.Reader, stopChan <-chan struct{}) error {
	return r.store.LoadSnapShotFromReader(reader, stopChan)
}

func (r *RocksDBStateMachine) Close() error {
	return r.store.Close()
}

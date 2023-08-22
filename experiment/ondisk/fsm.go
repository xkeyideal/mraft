package ondisk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/xkeyideal/mraft/experiment/store"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	appliedIndexKey = "disk_kv_applied_index"
	endSignal       = "mraft-end-signal"
)

type DiskKV struct {
	clusterID uint64
	nodeID    uint64

	dbIndex     uint32
	stores      []*store.Store
	lastApplied uint64
}

func NewDiskKV(cluserID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	return &DiskKV{
		clusterID: cluserID,
		nodeID:    nodeID,
		stores:    make([]*store.Store, 2),
	}
}

func (d *DiskKV) queryAppliedIndex() (uint64, error) {
	idx := atomic.LoadUint32(&d.dbIndex)

	return d.stores[idx].LookupAppliedIndex([]byte(appliedIndexKey))
}

func (d *DiskKV) Open(stopc <-chan struct{}) (uint64, error) {
	select {
	case <-stopc:
		return 0, sm.ErrOpenStopped
	default:
		dir := getNodeDBDirName(d.clusterID, d.nodeID)
		if err := createNodeDataDir(dir); err != nil {
			return 0, nil
		}

		var dbdir string
		if !isNewRun(dir) {
			if err := cleanupNodeDataDir(dir); err != nil {
				return 0, err
			}
			var err error
			dbdir, err = getCurrentDBDirName(dir)
			if err != nil {
				return 0, err
			}
			if _, err := os.Stat(dbdir); err != nil {
				if os.IsNotExist(err) {
					return 0, err
				}
			}
		} else {
			dbdir = getNewRandomDBDirName(dir)
			if err := saveCurrentDBDirName(dir, dbdir); err != nil {
				return 0, err
			}
			if err := replaceCurrentDBFile(dir); err != nil {
				return 0, err
			}
		}

		store, err := store.NewStore(dbdir)
		if err != nil {
			return 0, err
		}

		d.dbIndex = 0

		d.stores[d.dbIndex] = store
		appliedIndex, err := d.queryAppliedIndex()
		if err != nil {
			return 0, err
		}

		d.lastApplied = appliedIndex

		return appliedIndex, nil
	}
}

// Update 与 LookUp, SaveSnapshot的调用是并发安全的
func (d *DiskKV) Update(ents []sm.Entry) ([]sm.Entry, error) {

	if len(ents) == 0 {
		return ents, nil
	}

	dbIndex := atomic.LoadUint32(&d.dbIndex)
	db := d.stores[dbIndex]

	batch := db.Batch()
	defer batch.Close()

	for index, entry := range ents {
		if entry.Index <= d.lastApplied {
			continue
		}

		cmd := &store.Command{}
		err := cmd.Unmarshal(entry.Cmd)
		if err != nil {
			continue
		}

		switch cmd.Cmd {
		case store.CommandDelete:
			batch.Delete([]byte(cmd.Key), db.GetWo())
		case store.CommandUpsert:
			batch.Set([]byte(cmd.Key), []byte(cmd.Val), db.GetWo())
		default:
		}

		ents[index].Result = sm.Result{Value: uint64(len(ents[index].Cmd))}
	}

	idx := fmt.Sprintf("%d", ents[len(ents)-1].Index)
	batch.Set([]byte(appliedIndexKey), []byte(idx), db.GetWo())

	if err := db.Write(batch); err != nil {
		return nil, err
	}

	d.lastApplied = ents[len(ents)-1].Index

	return ents, nil
}

// Lookup 与 Update and RecoverFromSnapshot 是并发安全的
func (d *DiskKV) Lookup(key interface{}) (interface{}, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	if d.stores[dbIndex] != nil {
		v, err := d.stores[dbIndex].Lookup(key.([]byte))
		return v, err
	}
	return nil, errors.New("db is nil")
}

func (d *DiskKV) NALookup(key []byte) ([]byte, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	if d.stores[dbIndex] != nil {
		return d.stores[dbIndex].NALookup(key)
	}
	return nil, errors.New("db is nil")
}

type diskKVCtx struct {
	store    *store.Store
	snapshot *pebble.Snapshot
}

func (d *DiskKV) PrepareSnapshot() (interface{}, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	store := d.stores[dbIndex]

	return &diskKVCtx{
		store:    store,
		snapshot: store.NewSnapshot(),
	}, nil
}

func (d *DiskKV) saveToWriter(store *store.Store, snapshot *pebble.Snapshot, w io.Writer) error {
	iter := snapshot.NewIter(store.GetRo())
	defer iter.Close()

	keySize := make([]byte, 4)
	valSize := make([]byte, 4)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()

		kl := len(key)
		vl := len(val)

		binary.LittleEndian.PutUint32(keySize, uint32(kl))
		if _, err := w.Write(keySize); err != nil {
			return err
		}

		if _, err := w.Write(key); err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(valSize, uint32(vl))
		if _, err := w.Write(valSize); err != nil {
			return err
		}

		if _, err := w.Write(val); err != nil {
			return err
		}
	}

	return nil
}

func (d *DiskKV) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
		ctxdata := ctx.(*diskKVCtx)

		store := ctxdata.store
		ss := ctxdata.snapshot
		defer ss.Close()

		return d.saveToWriter(store, ss, w)
	}
}

// RecoverFromSnapshot 执行时，sm 的其他接口不会被同时执行
func (d *DiskKV) RecoverFromSnapshot(reader io.Reader, done <-chan struct{}) error {
	dir := getNodeDBDirName(d.clusterID, d.nodeID)
	dbdir := getNewRandomDBDirName(dir)
	oldDirName, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}

	store, err := store.NewStore(dbdir)
	if err != nil {
		return err
	}

	sz := make([]byte, 4)
	for {
		if isStop(done) {
			return sm.ErrSnapshotStopped
		}

		// 先读key
		_, err := io.ReadFull(reader, sz) // key size
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		toRead := binary.LittleEndian.Uint64(sz)
		kdata := make([]byte, toRead)
		_, err = io.ReadFull(reader, kdata) // key data
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// 再读val
		_, err = io.ReadFull(reader, sz) // val size
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		toRead = binary.LittleEndian.Uint64(sz)
		vdata := make([]byte, toRead)
		_, err = io.ReadFull(reader, vdata) // val data
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		store.SetKv(kdata, vdata)
	}

	store.Flush() // db 刷盘

	if err := saveCurrentDBDirName(dir, dbdir); err != nil {
		return err
	}
	if err := replaceCurrentDBFile(dir); err != nil {
		return err
	}

	oldDbIndex := atomic.LoadUint32(&d.dbIndex)
	newDbIndex := 1 - oldDbIndex
	atomic.StoreUint32(&d.dbIndex, newDbIndex)
	d.stores[newDbIndex] = store

	newLastApplied, err := d.queryAppliedIndex()
	if err != nil {
		return err
	}

	d.stores[oldDbIndex].Close()

	d.lastApplied = newLastApplied

	return os.RemoveAll(oldDirName)
}

func (d *DiskKV) Close() error {
	for i := 0; i < 2; i++ {
		if d.stores[i] != nil {
			d.stores[i].Close()
		}
	}

	return nil
}

func (d *DiskKV) Sync() error {
	return nil
}

func isStop(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

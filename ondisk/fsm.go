package ondisk

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/tecbot/gorocksdb"
)

const (
	appliedIndexKey string = "disk_kv_applied_index"
)

type KvCmd struct {
	Cmd string  `json:"cmd"`
	Kv  KvStore `json:"kv"`
}

type KvStore struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

type DiskKV struct {
	clusterID uint64
	nodeID    uint64

	dbIndex     uint32
	dbs         []*rocksdb
	lastApplied uint64
}

func NewDiskKV(cluserID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	return &DiskKV{
		clusterID: cluserID,
		nodeID:    nodeID,
		dbs:       make([]*rocksdb, 2),
	}
}

func (d *DiskKV) queryAppliedIndex() (uint64, error) {
	idx := atomic.LoadUint32(&d.dbIndex)
	val, err := d.dbs[idx].db.Get(d.dbs[idx].ro, []byte(appliedIndexKey))
	if err != nil {
		return 0, err
	}
	defer val.Free()

	data := val.Data()
	if len(data) == 0 {
		return 0, nil
	}

	return strconv.ParseUint(string(data), 10, 64)
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
					panic("db dir unexpectedly deleted")
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

		db, err := create(dbdir)
		if err != nil {
			return 0, err
		}

		d.dbIndex = 0

		d.dbs[d.dbIndex] = db
		appliedIndex, err := d.queryAppliedIndex()
		if err != nil {
			return 0, err
		}

		d.lastApplied = appliedIndex

		return 0, nil
	}
}

func (d *DiskKV) Update(ents []sm.Entry) ([]sm.Entry, error) {

	if len(ents) == 0 {
		return ents, nil
	}

	appliedIndex, err := d.queryAppliedIndex()
	if err != nil {
		return ents, err
	}

	wb := gorocksdb.NewWriteBatch()
	defer wb.Destroy()

	for index, entry := range ents {
		if uint64(index) <= appliedIndex {
			continue
		}

		data := &KvCmd{}
		err := json.Unmarshal(entry.Cmd, data)
		if err != nil {
			continue
		}

		switch data.Cmd {
		case "delete":
			wb.Delete([]byte(data.Kv.Key))
		case "update", "insert":
			wb.Put([]byte(data.Kv.Key), []byte(data.Kv.Val))
		}

		ents[index].Result = sm.Result{Value: uint64(len(ents[index].Cmd))}
	}

	idx := fmt.Sprintf("%d", ents[len(ents)-1].Index)
	wb.Put([]byte(appliedIndexKey), []byte(idx))

	dbIndex := atomic.LoadUint32(&d.dbIndex)
	if err := d.dbs[dbIndex].db.Write(d.dbs[dbIndex].wo, wb); err != nil {
		return nil, err
	}

	d.lastApplied = ents[len(ents)-1].Index

	return ents, nil
}

func (d *DiskKV) Lookup(key interface{}) (interface{}, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	if d.dbs[dbIndex] != nil {
		v, err := d.dbs[dbIndex].lookup(key.([]byte))
		return v, err
	}
	return nil, errors.New("db is nil")
}

type diskKVCtx struct {
	db       *rocksdb
	snapshot *gorocksdb.Snapshot
}

func (d *DiskKV) PrepareSnapshot() (interface{}, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	db := d.dbs[dbIndex]

	return &diskKVCtx{
		db:       db,
		snapshot: db.db.NewSnapshot(),
	}, nil
}

func (d *DiskKV) saveToWriter(db *rocksdb, ss *gorocksdb.Snapshot, w io.Writer) error {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetSnapshot(ss)
	iter := db.db.NewIterator(ro)
	defer iter.Close()

	sz := make([]byte, 8)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		dataKv := &KvStore{
			Key: string(key.Data()),
			Val: string(val.Data()),
		}
		data, err := json.Marshal(dataKv)
		if err != nil {
			continue
		}
		binary.LittleEndian.PutUint64(sz, uint64(len(data)))
		if _, err := w.Write(sz); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}

	endSignal := "mraft-end-signal"
	binary.LittleEndian.PutUint64(sz, uint64(len(endSignal)))
	if _, err := w.Write(sz); err != nil {
		return err
	}
	if _, err := w.Write([]byte(endSignal)); err != nil {
		return err
	}

	return nil
}

func (d *DiskKV) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
		ctxdata := ctx.(*diskKVCtx)

		db := ctxdata.db
		db.mu.RLock()
		defer db.mu.RUnlock()

		ss := ctxdata.snapshot

		return d.saveToWriter(db, ss, w)
	}
}

func (d *DiskKV) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
		dir := getNodeDBDirName(d.clusterID, d.nodeID)
		dbdir := getNewRandomDBDirName(dir)
		oldDirName, err := getCurrentDBDirName(dir)
		if err != nil {
			return err
		}

		db, err := create(dbdir)
		if err != nil {
			return err
		}

		sz := make([]byte, 8)
		endSignal := []byte("mraft-end-signal")
		wb := gorocksdb.NewWriteBatch()
		for {
			if _, err := io.ReadFull(r, sz); err != nil {
				return err
			}
			toRead := binary.LittleEndian.Uint64(sz)
			data := make([]byte, toRead)
			if _, err := io.ReadFull(r, data); err != nil {
				return err
			}

			if bytes.Compare(data, endSignal) == 0 {
				break
			}

			dataKv := &KvStore{}
			if err := json.Unmarshal(data, dataKv); err != nil {
				continue
			}
			wb.Put([]byte(dataKv.Key), []byte(dataKv.Val))
		}

		if err := db.db.Write(db.wo, wb); err != nil {
			return err
		}

		if err := saveCurrentDBDirName(dir, dbdir); err != nil {
			return err
		}
		if err := replaceCurrentDBFile(dir); err != nil {
			return err
		}

		oldDbIndex := atomic.LoadUint32(&d.dbIndex)
		newDbIndex := 1 - oldDbIndex
		atomic.StoreUint32(&d.dbIndex, newDbIndex)
		d.dbs[newDbIndex] = db

		newLastApplied, err := d.queryAppliedIndex()
		if err != nil {
			return err
		}

		d.dbs[oldDbIndex].close()

		d.lastApplied = newLastApplied

		return os.RemoveAll(oldDirName)
	}
}

func (d *DiskKV) Close() error {
	for i := 0; i < 2; i++ {
		if d.dbs[i] != nil {
			d.dbs[i].close()
		}
	}

	return nil
}

func (d *DiskKV) Sync() error {
	return nil
}

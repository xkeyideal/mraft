package simpleondisk

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mraft/store"
	"os"
	"strconv"
	"sync/atomic"

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/tecbot/gorocksdb"
)

const (
	appliedIndexKey = "disk_kv_applied_index"
	endSignal       = "mraft-end-signal"
)

type kv struct {
	Key string `json:"key"`
	Val int    `json:"val"`
}

type SimpleDiskKV struct {
	clusterID uint64
	nodeID    uint64

	dbIndex     uint32
	stores      []*store.Store
	lastApplied uint64
}

func NewSimpleDiskKV(cluserID uint64, nodeID uint64) sm.IOnDiskStateMachine {
	return &SimpleDiskKV{
		clusterID: cluserID,
		nodeID:    nodeID,
		stores:    make([]*store.Store, 2),
	}
}

func (d *SimpleDiskKV) queryAppliedIndex() (uint64, error) {
	idx := atomic.LoadUint32(&d.dbIndex)

	return d.stores[idx].LookupAppliedIndex([]byte(appliedIndexKey))
}

func (d *SimpleDiskKV) Open(stopc <-chan struct{}) (uint64, error) {
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
func (d *SimpleDiskKV) Update(ents []sm.Entry) ([]sm.Entry, error) {

	fmt.Println("SimpleDiskKV Entry length: ", len(ents))

	if len(ents) == 0 {
		return ents, nil
	}

	dbIndex := atomic.LoadUint32(&d.dbIndex)

	for index, entry := range ents {
		if entry.Index <= d.lastApplied {
			continue
		}

		data := &kv{}
		json.Unmarshal(entry.Cmd, data)

		oldVal, err := d.NALookup([]byte(data.Key))

		if err != nil {
			d.stores[dbIndex].Write([]byte(data.Key), []byte(strconv.Itoa(data.Val)))
		} else {
			v, err := strconv.ParseInt(string(oldVal), 10, 32)
			if err != nil {
				fmt.Printf("%s ParseInt %s", string(oldVal), err.Error())
				continue
			}

			d.stores[dbIndex].Write([]byte(data.Key), []byte(strconv.Itoa(data.Val+int(v))))
		}

		ents[index].Result = sm.Result{Value: uint64(len(ents[index].Cmd))}
	}

	idx := fmt.Sprintf("%d", ents[len(ents)-1].Index)
	d.stores[dbIndex].Write([]byte(appliedIndexKey), []byte(idx))

	d.lastApplied = ents[len(ents)-1].Index

	return ents, nil
}

// Lookup 与 Update and RecoverFromSnapshot 是并发安全的
func (d *SimpleDiskKV) Lookup(key interface{}) (interface{}, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	if d.stores[dbIndex] != nil {
		v, err := d.stores[dbIndex].NALookup(key.([]byte))
		return v, err
	}
	return nil, errors.New("db is nil")
}

func (d *SimpleDiskKV) NALookup(key []byte) ([]byte, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	if d.stores[dbIndex] != nil {
		return d.stores[dbIndex].NALookup(key)
	}
	return nil, errors.New("db is nil")
}

type diskKVCtx struct {
	store    *store.Store
	snapshot *gorocksdb.Snapshot
}

func (d *SimpleDiskKV) PrepareSnapshot() (interface{}, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	store := d.stores[dbIndex]

	return &diskKVCtx{
		store:    store,
		snapshot: store.NewSnapshot(),
	}, nil
}

func (d *SimpleDiskKV) saveToWriter(store *store.Store, ss *gorocksdb.Snapshot, w io.Writer) error {
	iter := store.NewIterator(ss)
	defer iter.Close()
	defer store.ReleaseSnapshot(ss)

	dataSize := make([]byte, 4)
	keySize := make([]byte, 4)
	valSize := make([]byte, 4)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		val := iter.Value().Data()

		kl := iter.Key().Size()
		vl := iter.Value().Size()

		binary.LittleEndian.PutUint32(dataSize, uint32(kl+vl+4+4))
		if _, err := w.Write(dataSize); err != nil {
			return err
		}

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

	binary.LittleEndian.PutUint32(dataSize, uint32(len(endSignal)))
	if _, err := w.Write(dataSize); err != nil {
		return err
	}
	if _, err := w.Write([]byte(endSignal)); err != nil {
		return err
	}

	return nil
}

func (d *SimpleDiskKV) SaveSnapshot(ctx interface{}, w io.Writer, done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
		ctxdata := ctx.(*diskKVCtx)

		store := ctxdata.store
		ss := ctxdata.snapshot

		return d.saveToWriter(store, ss, w)
	}
}

// RecoverFromSnapshot 执行时，sm 的其他接口不会被同时执行
func (d *SimpleDiskKV) RecoverFromSnapshot(r io.Reader, done <-chan struct{}) error {
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

		store, err := store.NewStore(dbdir)
		if err != nil {
			return err
		}

		sz := make([]byte, 4)
		wb := gorocksdb.NewWriteBatch()
		for {
			if _, err := io.ReadFull(r, sz); err != nil {
				return err
			}
			dataSize := binary.LittleEndian.Uint32(sz)
			data := make([]byte, dataSize)
			if _, err := io.ReadFull(r, data); err != nil {
				return err
			}

			if bytes.Compare(data, []byte(endSignal)) == 0 {
				break
			}

			kl := binary.LittleEndian.Uint32(data[:4])
			key := data[4 : kl+4]
			vl := binary.LittleEndian.Uint32(data[kl+4 : kl+8])
			val := data[kl+8:]
			if uint32(len(val)) != vl {
				continue
			}
			wb.Put(key, val)
		}

		if err := store.BatchWrite(wb); err != nil {
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
		d.stores[newDbIndex] = store

		newLastApplied, err := d.queryAppliedIndex()
		if err != nil {
			return err
		}

		d.stores[oldDbIndex].Close()

		d.lastApplied = newLastApplied

		return os.RemoveAll(oldDirName)
	}
}

func (d *SimpleDiskKV) Close() error {
	for i := 0; i < 2; i++ {
		if d.stores[i] != nil {
			d.stores[i].Close()
		}
	}

	return nil
}

func (d *SimpleDiskKV) Sync() error {
	return nil
}

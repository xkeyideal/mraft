package simpleondisk

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/xkeyideal/mraft/experiment/store"

	sm "github.com/lni/dragonboat/v3/statemachine"
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
			return 0, err
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

		st, err := store.NewStore(dbdir)
		if err != nil {
			return 0, err
		}

		d.dbIndex = 0

		d.stores[d.dbIndex] = st
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
	db := d.stores[dbIndex]

	batch := db.Batch()
	defer batch.Close()

	// 同一批 entry 中对相同 key 的更新必须互相可见
	pending := make(map[string]int)

	for index, entry := range ents {
		if entry.Index <= d.lastApplied {
			continue
		}

		data := &kv{}
		if err := json.Unmarshal(entry.Cmd, data); err != nil {
			return ents, err
		}

		cur := 0
		if v, ok := pending[data.Key]; ok {
			cur = v
		} else {
			oldVal, err := d.NALookup([]byte(data.Key))
			if err != nil {
				if err != pebble.ErrNotFound {
					return ents, err
				}
			} else {
				v, err := strconv.ParseInt(string(oldVal), 10, 32)
				if err != nil {
					return ents, fmt.Errorf("parse old value %q: %w", string(oldVal), err)
				}
				cur = int(v)
			}
		}

		newVal := cur + data.Val
		pending[data.Key] = newVal

		if err := batch.Set([]byte(data.Key), []byte(strconv.Itoa(newVal)), nil); err != nil {
			return ents, err
		}

		ents[index].Result = sm.Result{Value: uint64(len(ents[index].Cmd))}
	}

	idx := fmt.Sprintf("%d", ents[len(ents)-1].Index)
	if err := batch.Set([]byte(appliedIndexKey), []byte(idx), nil); err != nil {
		return ents, err
	}

	if err := db.Write(batch); err != nil {
		return ents, err
	}

	d.lastApplied = ents[len(ents)-1].Index

	return ents, nil
}

// Lookup 与 Update and RecoverFromSnapshot 是并发安全的
func (d *SimpleDiskKV) Lookup(key interface{}) (interface{}, error) {
	k, ok := key.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid key type: %T", key)
	}

	dbIndex := atomic.LoadUint32(&d.dbIndex)
	if d.stores[dbIndex] != nil {
		return d.stores[dbIndex].NALookup(k)
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
	snapshot *pebble.Snapshot
}

func (d *SimpleDiskKV) PrepareSnapshot() (interface{}, error) {
	dbIndex := atomic.LoadUint32(&d.dbIndex)
	store := d.stores[dbIndex]

	return &diskKVCtx{
		store:    store,
		snapshot: store.NewSnapshot(),
	}, nil
}

func (d *SimpleDiskKV) saveToWriter(store *store.Store, snapshot *pebble.Snapshot, w io.Writer, done <-chan struct{}) error {
	iter := snapshot.NewIter(store.GetRo())
	defer iter.Close()

	keySize := make([]byte, 4)
	valSize := make([]byte, 4)
	for iter.First(); iter.Valid(); iter.Next() {
		if isStop(done) {
			return errors.New("snapshot stopped")
		}

		key := iter.Key()
		val := iter.Value()

		kl := len(key)
		vl := len(val)

		binary.LittleEndian.PutUint32(keySize, uint32(kl))
		if err := writeAll(w, keySize); err != nil {
			return err
		}

		if err := writeAll(w, key); err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(valSize, uint32(vl))
		if err := writeAll(w, valSize); err != nil {
			return err
		}

		if err := writeAll(w, val); err != nil {
			return err
		}
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

		defer ss.Close()

		return d.saveToWriter(store, ss, w, done)
	}
}

// RecoverFromSnapshot 执行时，sm 的其他接口不会被同时执行
func (d *SimpleDiskKV) RecoverFromSnapshot(reader io.Reader, done <-chan struct{}) error {
	dir := getNodeDBDirName(d.clusterID, d.nodeID)
	dbdir := getNewRandomDBDirName(dir)
	oldDirName, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}

	st, err := store.NewStore(dbdir)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			st.Close()
		}
	}()

	sz := make([]byte, 4)
	for {
		if isStop(done) {
			return sm.ErrSnapshotStopped
		}

		// 只有读第一个 key-size 头时才允许干净的 EOF
		_, err := io.ReadFull(reader, sz)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		toRead := binary.LittleEndian.Uint32(sz)
		kdata := make([]byte, toRead)
		if _, err = io.ReadFull(reader, kdata); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		// 再读val size
		if _, err = io.ReadFull(reader, sz); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		toRead = binary.LittleEndian.Uint32(sz)
		vdata := make([]byte, toRead)
		if _, err = io.ReadFull(reader, vdata); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		if err := st.SetKv(kdata, vdata); err != nil {
			return err
		}
	}

	if err := st.Flush(); err != nil {
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
	d.stores[newDbIndex] = st
	cleanup = false

	newLastApplied, err := d.queryAppliedIndex()
	if err != nil {
		return err
	}

	if old := d.stores[oldDbIndex]; old != nil {
		old.Close()
	}

	d.lastApplied = newLastApplied

	return os.RemoveAll(oldDirName)
}

func (d *SimpleDiskKV) Close() error {
	var err error
	for i := 0; i < 2; i++ {
		if d.stores[i] != nil {
			if cerr := d.stores[i].Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	}

	return err
}

func (d *SimpleDiskKV) Sync() error {
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

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

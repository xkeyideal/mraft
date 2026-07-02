package store

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/xkeyideal/gokit/tools"
	"go.uber.org/zap"
)

var ColumnFamilyDefault = "default"
var PebbleColumnFamilyMap = map[string]byte{
	ColumnFamilyDefault: 1,
}

type Store struct {
	clusterId uint64
	// data_nodexxxxx/clusterId
	path string
	opts PebbleClusterOption
	log  *zap.Logger

	dbMu sync.RWMutex
	db   *atomic.Pointer[pebble.DB]

	ro     *pebble.IterOptions
	wo     *pebble.WriteOptions
	syncwo *pebble.WriteOptions

	closed *atomic.Bool
}

func NewStore(clusterId uint64, path, pebbleDBDir string, opts PebbleClusterOption, log *zap.Logger) (*Store, error) {
	cfg := getDefaultPebbleDBConfig()

	db, err := openPebbleDB(cfg, pebbleDBDir, opts, log)
	if err != nil {
		return nil, err
	}

	return &Store{
		clusterId: clusterId,
		path:      path,
		log:       log,
		opts:      opts,
		db:        atomic.NewPointer[pebble.DB](db),
		ro:        &pebble.IterOptions{},
		wo:        &pebble.WriteOptions{Sync: false},
		syncwo:    &pebble.WriteOptions{Sync: true},
		closed:    atomic.NewBool(false),
	}, nil
}

func (s *Store) Closed() bool {
	return s.closed.Load()
}

func (s *Store) GetColumnFamily(cf string) (byte, error) {
	v, ok := PebbleColumnFamilyMap[cf]
	if !ok {
		return 0, fmt.Errorf("unknown column family: %s", cf)
	}
	return v, nil
}

func (s *Store) BuildColumnFamilyKey(cf byte, key []byte) []byte {
	return append([]byte{cf}, key...)
}

func (s *Store) GetBytes(key []byte) ([]byte, error) {
	if s.closed.Load() {
		return []byte{}, pebble.ErrClosed
	}

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.db.Load()
	if db == nil {
		return nil, pebble.ErrClosed
	}
	val, closer, err := db.Get(key)

	// 查询的key不存在，返回空值
	if err == pebble.ErrNotFound {
		return []byte{}, nil
	}

	if err != nil {
		return nil, err
	}

	// 这里需要copy
	data := make([]byte, len(val))
	copy(data, val)

	if err := closer.Close(); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *Store) Batch() *pebble.Batch {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.db.Load()
	if db == nil {
		return nil
	}
	return db.NewBatch()
}

func (s *Store) Write(b *pebble.Batch) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	if s.db.Load() == nil {
		return pebble.ErrClosed
	}
	return b.Commit(s.syncwo)
}

func (s *Store) GetWo() *pebble.WriteOptions {
	return s.wo
}

func (s *Store) GetIterator() *pebble.Iterator {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.db.Load()
	if db == nil {
		return nil
	}
	return db.NewIter(s.ro)
}

func (s *Store) GetSnapshot() *pebble.Snapshot {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.db.Load()
	if db == nil {
		return nil
	}
	return db.NewSnapshot()
}

type KVData struct {
	Key string `json:"k"`
	Val string `json:"v"`
}

func iteratorIsValid(iter *pebble.Iterator) bool {
	return iter.Valid()
}

func (s *Store) SaveSnapshotToWriter(target, raftAddr string, snapshot *pebble.Snapshot, w io.Writer, stopChan <-chan struct{}) error {
	iter := snapshot.NewIter(s.ro)
	defer iter.Close()

	start := tools.CSTNow()

	// 将db里的数据全部遍历进内存
	var count uint64 = 0
	sz := make([]byte, 8)

	// 遍历pebblebd snapshot, 将数据写入 io.Writer
	for iter.First(); iteratorIsValid(iter); iter.Next() {
		if isStop(stopChan) {
			return errors.New("snapshot stopped")
		}

		key := iter.Key()
		val := iter.Value()

		count++

		// 先写key
		binary.LittleEndian.PutUint64(sz, uint64(len(key)))
		if err := writeAll(w, sz); err != nil { // key size
			return err
		}
		if err := writeAll(w, key); err != nil { // key data
			return err
		}

		// 再写value
		// gzip encode
		gzipVal, err := gzipEncode(val)
		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint64(sz, uint64(len(gzipVal)))
		if err := writeAll(w, sz); err != nil { // val size
			return err
		}
		if err := writeAll(w, gzipVal); err != nil { // val data
			return err
		}
	}

	s.log.Warn(fmt.Sprintf("[raftstorage] [SaveSnapshot] [SaveSnapShot] [%s] [%s]", target, raftAddr),
		zap.String("dbdir", s.path),
		zap.Uint64("clusterId", s.clusterId),
		zap.String("startTime", start.String()),
		zap.String("endTime", tools.CSTNow().String()),
		zap.Int64("cost", int64(tools.CSTNow().Sub(start))/1e6),
		zap.Uint64("count", count),
	)

	return nil
}

func (s *Store) LoadSnapShotFromReader(target, raftAddr string, reader io.Reader, stopChan <-chan struct{}) error {
	// data_nodexxxxx/clusterId/uuid
	dbdir := getNewRandomDBDirName(s.path)

	name, err := getCurrentDBDirName(s.path) // 从存储里拿到当前的current db目录
	if err != nil {
		return err
	}
	oldDirName := name

	newdb, err := openPebbleDB(getDefaultPebbleDBConfig(), dbdir, s.opts, s.log)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			newdb.Close()
		}
	}()

	var count uint64
	sz := make([]byte, 8)
	start := time.Now()

	batch := newdb.NewBatch()
	defer batch.Close()

	// 开始从snapshot reader里读取数据, 等到EOF退出
	for {
		if isStop(stopChan) {
			return errors.New("snapshot stopped")
		}

		// 只有读第一个 key-size 头时才允许干净的 EOF
		_, err := io.ReadFull(reader, sz) // key size
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		toRead := binary.LittleEndian.Uint64(sz)
		kdata := make([]byte, toRead)
		if _, err = io.ReadFull(reader, kdata); err != nil { // key data
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		// 再读val size
		if _, err = io.ReadFull(reader, sz); err != nil { // val size
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		toRead = binary.LittleEndian.Uint64(sz)
		vdata := make([]byte, toRead)
		if _, err = io.ReadFull(reader, vdata); err != nil { // val data
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		// gzip decode
		ungzipVData, err := gzipDecode(vdata)
		if err != nil {
			return err
		}

		// 写入batch
		if err := batch.Set(kdata, ungzipVData, nil); err != nil {
			return err
		}
		count++
	}

	// 同步写入db
	if err := newdb.Apply(batch, s.syncwo); err != nil {
		return err
	}
	if err := newdb.Flush(); err != nil {
		return err
	}

	if err := saveCurrentDBDirName(s.path, dbdir); err != nil {
		return err
	}
	if err := replaceCurrentDBFile(s.path); err != nil {
		return err
	}

	// 用新db置换老db，并close 老db
	s.dbMu.Lock()
	old := s.db.Swap(newdb)
	if old != nil {
		old.Close()
	}
	s.dbMu.Unlock()
	cleanup = false

	// 删除旧的pebbledb存储的文件数据
	if err := os.RemoveAll(oldDirName); err != nil {
		return err
	}

	s.log.Warn(fmt.Sprintf("[raftstorage] [RecoverFromSnapshot] [RecoverFromSnapshot] [%s] [%s]", target, raftAddr),
		zap.String("dbdir", s.path),
		zap.Uint64("clusterId", s.clusterId),
		zap.String("startTime", start.String()),
		zap.String("endTime", tools.CSTNow().String()),
		zap.Int64("cost", int64(tools.CSTNow().Sub(start))/1e6),
		zap.Uint64("count", count),
		zap.String("newdbdir", dbdir),
		zap.String("olddbdir", oldDirName),
	)

	parent := filepath.Dir(oldDirName)
	return syncDir(parent)
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}

	s.closed.Store(true) // set pebbledb closed
	s.log.Sync()

	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	var err error
	db := s.db.Load()
	if db != nil {
		if ferr := db.Flush(); ferr != nil && err == nil {
			err = ferr
		}
		if cerr := db.Close(); cerr != nil && err == nil {
			err = cerr
		}
		s.db.Store(nil)
	}

	return err
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

func gzipEncode(content []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(content); err != nil {
		return nil, err
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func gzipDecode(gzipMsg []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewBuffer(gzipMsg))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

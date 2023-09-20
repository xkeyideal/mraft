package store

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	db   *atomic.Pointer[pebble.DB]

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
		closed:    atomic.NewBool(false),
	}, nil
}

func (s *Store) Closed() bool {
	return s.closed.Load()
}

func (s *Store) GetColumnFamily(cf string) byte {
	return PebbleColumnFamilyMap[cf]
}

func (s *Store) BuildColumnFamilyKey(cf byte, key []byte) []byte {
	return append([]byte{cf}, key...)
}

func (s *Store) GetBytes(key []byte) ([]byte, error) {
	if s.closed.Load() {
		return []byte{}, pebble.ErrClosed
	}

	db := s.db.Load()
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
	db := s.db.Load()
	return db.NewBatch()
}

func (s *Store) Write(b *pebble.Batch) error {
	return b.Commit(pebble.Sync)
}

func (s *Store) GetIterator() *pebble.Iterator {
	db := s.db.Load()
	return db.NewIter(&pebble.IterOptions{})
}

func (s *Store) GetSnapshot() *pebble.Snapshot {
	db := s.db.Load()
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
	iter := snapshot.NewIter(&pebble.IterOptions{})
	defer iter.Close()

	start := tools.CSTNow()

	// 将db里的数据全部遍历进内存
	var count uint64 = 0
	sz := make([]byte, 8)

	// 遍历pebblebd snapshot, 将数据写入 io.Writer
	for iter.First(); iteratorIsValid(iter); iter.Next() {
		key := iter.Key()
		val := iter.Value()

		count++

		// 先写key
		binary.LittleEndian.PutUint64(sz, uint64(len(key)))
		if _, err := w.Write(sz); err != nil { // key size
			return err
		}
		if _, err := w.Write(key); err != nil { // key data
			return err
		}

		// 再写value
		// gzip encode
		gzipVal, err := gzipEncode(val)
		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint64(sz, uint64(len(gzipVal)))
		if _, err := w.Write(sz); err != nil { // val size
			return err
		}
		if _, err := w.Write(gzipVal); err != nil { // val data
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

	var oldDirName string

	// 为了兼容现有的采用data_nodexxxxx/clusterId/current 作为当前pebbledb的存储目录
	// fp := filepath.Join(s.path, "current")
	// if existFilePath(fp) {
	// 	oldDirName = fp
	// } else {
	// 	name, err := getCurrentDBDirName(s.path) // 从存储里拿到当前的current db目录
	// 	if err != nil {
	// 		return err
	// 	}
	// 	oldDirName = name
	// }

	name, err := getCurrentDBDirName(s.path) // 从存储里拿到当前的current db目录
	if err != nil {
		return err
	}
	oldDirName = name

	newdb, err := openPebbleDB(getDefaultPebbleDBConfig(), dbdir, s.opts, s.log)
	if err != nil {
		return err
	}

	var count uint64 = 0
	sz := make([]byte, 8)
	start := time.Now()

	// 开始从snapshot reader里读取数据, 等到EOF退出
	for {
		if isStop(stopChan) {
			return nil
		}

		count++

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

		// gzip decode
		ungzipVData, err := gzipDecode(vdata)
		if err != nil {
			continue
		}

		// 写入db
		newdb.Set(kdata, ungzipVData, pebble.Sync)
	}

	// 同步写入db
	// if err := newdb.Apply(wb, s.syncwo); err != nil {
	// 	return err
	// }
	newdb.Flush() // db刷盘

	if err := saveCurrentDBDirName(s.path, dbdir); err != nil {
		return err
	}
	if err := replaceCurrentDBFile(s.path); err != nil {
		return err
	}

	// 用新db置换老db，并close 老db
	old := s.db.Swap(newdb)
	if old != nil {
		old.Close()
	}

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

	db := s.db.Load()
	if db != nil {
		db.Flush()
		db.Close()
		db = nil
	}

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

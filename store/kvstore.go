package store

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/tecbot/gorocksdb"
)

type Store struct {
	mu     sync.RWMutex
	db     *gorocksdb.DB
	ro     *gorocksdb.ReadOptions
	wo     *gorocksdb.WriteOptions
	opts   *gorocksdb.Options
	closed bool
}

func NewStore(dbdir string) (*Store, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(640 * 1024 * 1024)) // 内存缓存大小 640MB
	bbto.SetBlockSize(512 * 1024)                                // block大小 512KB

	opts := gorocksdb.NewDefaultOptions()

	opts.SetNumLevels(6)
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetUseFsync(true)
	opts.SetWalSizeLimitMb(3000)               // rocksdb的wal日志大小
	opts.SetWriteBufferSize(128 * 1024 * 1024) // 写入缓冲区的大小, 128MB
	opts.SetMaxWriteBufferNumber(4)
	opts.SetMinWriteBufferNumberToMerge(2)
	opts.SetInfoLogLevel(gorocksdb.ErrorInfoLogLevel) // rocksdb日志级别
	opts.SetBytesPerSync(1048576)
	opts.SetMaxBackgroundFlushes(4)      // 后台flush线程数
	opts.SetMaxBackgroundCompactions(12) // 后台整理线程的个数, rocksdb的gc
	opts.SetLevelCompactionDynamicLevelBytes(true)

	// rocksdb_level0_file_num_compaction_trigger = 4
	// rocksdb_level0_slowdown_writes_trigger = 30
	// rocksdb_level0_stop_writes_trigger = 60
	opts.SetLevel0FileNumCompactionTrigger(4)   // Level-0层 当有4个未进行Compact的文件时，达到触发Compact的条件
	opts.SetLevel0SlowdownWritesTrigger(30)     // Level-0层 当有30个未进行Compact的文件时，触发RocksDB，减慢写入速度
	opts.SetLevel0StopWritesTrigger(60)         // Level-0层 当有60个未进行Compact的文件时，触发RocksDB停止写入文件，此时会尽快的Compact Level-0层文件
	opts.SetTargetFileSizeBase(2 * 1024 * 1024) // 2MB
	opts.SetTargetFileSizeMultiplier(1)

	// Level-1 层 文件总大小由 max_bytes_for_level_base 参数控制，而 Level-2 层的大小通过： Level_max_bytes[N] = Level_max_bytes[N-1] * max_bytes_for_level_multiplier^(N-1)*max_bytes_for_level_multiplier_additional[N-1]
	opts.SetMaxBytesForLevelBase(20 * 1024 * 1024) // 20MB 用于指定Level-1层总大小，超过这个值满足触发Compact条件
	opts.SetMaxBytesForLevelMultiplier(10)         // 每一层最大Bytes 乘法因子

	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)
	ro := gorocksdb.NewDefaultReadOptions()

	db, err := gorocksdb.OpenDb(opts, dbdir)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:   db,
		ro:   ro,
		wo:   wo,
		opts: opts,
	}, nil
}

func (db *Store) LookupAppliedIndex(key []byte) (uint64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, err := db.db.Get(db.ro, key)
	if err != nil {
		return 0, err
	}
	defer val.Free()

	data := val.Data()
	if len(data) == 0 {
		return 0, nil
	}

	v := make([]byte, val.Size())
	copy(v, data)

	return strconv.ParseUint(string(v), 10, 64)
}

func (db *Store) Lookup(key []byte) (*RaftAttribute, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, err := db.db.Get(db.ro, key)
	if err != nil {
		return nil, err
	}
	defer val.Free()

	data := val.Data()
	if len(data) == 0 {
		return nil, fmt.Errorf("key: <%s> not found", string(key))
	}

	v := make([]byte, val.Size())
	copy(v, data)

	attr := &RaftAttribute{}
	err = attr.Unmarshal(v)

	return attr, err
}

func (db *Store) NALookup(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, err := db.db.Get(db.ro, key)
	if err != nil {
		return nil, err
	}
	defer val.Free()

	data := val.Data()
	if len(data) == 0 {
		return nil, fmt.Errorf("key: <%s> not found", string(key))
	}

	v := make([]byte, val.Size())
	copy(v, data)

	return v, err
}

func (db *Store) BatchWrite(wb *gorocksdb.WriteBatch) error {
	return db.db.Write(db.wo, wb)
}

func (db *Store) Write(key, value []byte) error {
	return db.db.Put(db.wo, key, value)
}

func (db *Store) NewSnapshot() *gorocksdb.Snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.NewSnapshot()
}

func (db *Store) ReleaseSnapshot(snap *gorocksdb.Snapshot) {
	db.db.ReleaseSnapshot(snap)
}

func (db *Store) NewIterator(ss *gorocksdb.Snapshot) *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
	// Callers may wish to set this field to false for bulk scans. Default: true
	ro.SetFillCache(false)
	ro.SetSnapshot(ss)

	return db.db.NewIterator(ro)
}

func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	if s.db != nil {
		s.db.Close()
	}

	if s.opts != nil {
		s.opts.Destroy()
	}

	if s.wo != nil {
		s.wo.Destroy()
	}

	if s.ro != nil {
		s.ro.Destroy()
	}

	s.db = nil
}

package store

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cockroachdb/pebble"
	"go.uber.org/atomic"
)

type Store struct {
	mu     sync.RWMutex
	db     *pebble.DB
	ro     *pebble.IterOptions
	wo     *pebble.WriteOptions
	syncwo *pebble.WriteOptions
	closed *atomic.Bool
}

func NewStore(dbdir string) (*Store, error) {
	cfg := getDefaultPebbleDBConfig()

	db, err := openPebbleDB(cfg, dbdir)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:     db,
		ro:     &pebble.IterOptions{},
		wo:     &pebble.WriteOptions{Sync: false},
		syncwo: &pebble.WriteOptions{Sync: true},
		closed: atomic.NewBool(false),
	}, nil
}

func (db *Store) LookupAppliedIndex(key []byte) (uint64, error) {
	if db.closed.Load() {
		return 0, pebble.ErrClosed
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	val, closer, err := db.db.Get(key)
	if err != nil {
		return 0, err
	}

	// 这里需要copy
	data := make([]byte, len(val))
	copy(data, val)

	if err := closer.Close(); err != nil {
		return 0, err
	}

	return strconv.ParseUint(string(data), 10, 64)
}

func (db *Store) Lookup(key []byte) (*RaftAttribute, error) {
	if db.closed.Load() {
		return nil, pebble.ErrClosed
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	val, closer, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 这里需要copy
	data := make([]byte, len(val))
	copy(data, val)

	if err := closer.Close(); err != nil {
		return nil, err
	}

	attr := &RaftAttribute{}
	err = attr.Unmarshal(data)

	return attr, err
}

func (db *Store) NALookup(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, closer, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 这里需要copy
	data := make([]byte, len(val))
	copy(data, val)

	if err := closer.Close(); err != nil {
		return nil, err
	}

	return data, err
}

func (db *Store) Batch() *pebble.Batch {
	return db.db.NewBatch()
}

func (db *Store) GetWo() *pebble.WriteOptions {
	return db.wo
}

func (db *Store) GetRo() *pebble.IterOptions {
	return db.ro
}

func (db *Store) Write(b *pebble.Batch) error {
	return b.Commit(db.wo)
}

func (db *Store) SetKv(key, val []byte) {
	db.db.Set(key, val, db.wo)
}

func (db *Store) Flush() {
	db.db.Flush()
}

func (db *Store) NewSnapshot() *pebble.Snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.NewSnapshot()
}

func (db *Store) ReleaseSnapshot(snap *pebble.Snapshot) {
	snap.Close()
}

func (db *Store) GetIterator() *pebble.Iterator {
	return db.db.NewIter(db.ro)
}

func (db *Store) Close() error {
	if db == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.closed.Store(true) // set pebbledb closed

	if db.db != nil {
		db.db.Flush()
		db.db.Close()
		db.db = nil
	}

	return nil
}

type PebbleDBConfig struct {
	KVLRUCacheSize                   int64
	KVWriteBufferSize                int
	KVMaxWriteBufferNumber           int
	KVLevel0FileNumCompactionTrigger int
	KVLevel0StopWritesTrigger        int
	KVMaxBytesForLevelBase           int64
	KVTargetFileSizeBase             int64
	KVTargetFileSizeMultiplier       int64
	KVNumOfLevels                    int
	KVMaxOpenFiles                   int
	KVMaxConcurrentCompactions       int
	KVBlockSize                      int
	KVMaxManifestFileSize            int64
	KVBytesPerSync                   int
	KVWALBytesPerSync                int
}

func getDefaultPebbleDBConfig() PebbleDBConfig {
	return PebbleDBConfig{
		KVLRUCacheSize:                   128 * 1024 * 1024, // 128MB
		KVWriteBufferSize:                32 * 1024 * 1024,  // 32MB
		KVMaxWriteBufferNumber:           4,
		KVLevel0FileNumCompactionTrigger: 1,
		KVLevel0StopWritesTrigger:        24,
		KVMaxBytesForLevelBase:           512 * 1024 * 1024, // 512MB
		KVTargetFileSizeBase:             128 * 1024 * 1024, // 128MB
		KVTargetFileSizeMultiplier:       1,
		KVNumOfLevels:                    7,
		KVMaxOpenFiles:                   102400,
		KVMaxConcurrentCompactions:       8,
		KVBlockSize:                      64 * 1024,         // 64KB
		KVMaxManifestFileSize:            128 * 1024 * 1024, // 128MB
		KVBytesPerSync:                   2 * 1024 * 1024,   // 2MB
		KVWALBytesPerSync:                2 * 1024 * 1024,   // 2MB
	}
}

func openPebbleDB(config PebbleDBConfig, dir string) (*pebble.DB, error) {
	blockSize := config.KVBlockSize
	levelSizeMultiplier := config.KVTargetFileSizeMultiplier
	sz := config.KVTargetFileSizeBase
	lopts := make([]pebble.LevelOptions, 0)

	for l := 0; l < config.KVNumOfLevels; l++ {
		opt := pebble.LevelOptions{
			Compression:    pebble.DefaultCompression,
			BlockSize:      blockSize,
			TargetFileSize: sz,
		}
		sz = sz * levelSizeMultiplier
		lopts = append(lopts, opt)
	}

	dataPath := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walPath, os.ModePerm); err != nil {
		return nil, err
	}

	cache := pebble.NewCache(config.KVLRUCacheSize)
	opts := &pebble.Options{
		BytesPerSync:                config.KVBytesPerSync,
		Levels:                      lopts,
		MaxManifestFileSize:         config.KVMaxManifestFileSize,
		MemTableSize:                config.KVWriteBufferSize,
		MemTableStopWritesThreshold: config.KVMaxWriteBufferNumber,
		LBaseMaxBytes:               config.KVMaxBytesForLevelBase,
		L0CompactionThreshold:       config.KVLevel0FileNumCompactionTrigger,
		L0StopWritesThreshold:       config.KVLevel0StopWritesTrigger,
		Cache:                       cache,
		WALDir:                      walPath,
		MaxOpenFiles:                config.KVMaxOpenFiles,
		MaxConcurrentCompactions:    config.KVMaxConcurrentCompactions,
		WALBytesPerSync:             config.KVWALBytesPerSync,
	}

	db, err := pebble.Open(dataPath, opts)
	if err != nil {
		return nil, err
	}
	cache.Unref()

	return db, nil
}

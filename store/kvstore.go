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
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetUseFsync(true)

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

func (db *Store) BatchWrite(wb *gorocksdb.WriteBatch) error {
	return db.db.Write(db.wo, wb)
}

func (db *Store) NewSnapshot() *gorocksdb.Snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.NewSnapshot()
}

func (db *Store) NewIterator(ss *gorocksdb.Snapshot) *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
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

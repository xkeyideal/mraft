package store

import (
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

func (db *Store) Lookup(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, err := db.db.Get(db.ro, key)

	if err != nil {
		return nil, err
	}

	defer val.Free()

	data := val.Data()

	if len(data) == 0 {
		return []byte(""), nil
	}
	v := make([]byte, len(data))
	copy(v, data)
	return v, nil
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

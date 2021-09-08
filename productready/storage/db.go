package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/tecbot/gorocksdb"
)

type Store struct {
	db    *gorocksdb.DB
	path  string
	cfMap map[string]*gorocksdb.ColumnFamilyHandle
	cfs   []string
}

// path: base/data_node_nodeId/clusterId
func newStore(path string, cfs []string) (*Store, error) {
	dbPath := filepath.Join(path, "current")

	db, cfMap, err := createOrOpenRocksDB(dbPath, cfs)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:    db,
		cfMap: cfMap,
		cfs:   cfs,
		path:  path,
	}, nil
}

func newReadonlyStore(path string, cfs []string) (*Store, error) {
	db, cfMap, err := openReadonlyRocksDB(path, cfs)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, cfMap: cfMap, cfs: cfs, path: path}, nil
}

func (s *Store) GetBytes(cf string, key []byte) ([]byte, error) {
	cfHandle, ok := s.cfMap[cf]
	if !ok {
		return nil, errors.New("can not find cf:" + cf)
	}

	slice, err := s.db.GetCF(readOpt, cfHandle, key)
	if err != nil {
		return nil, err
	}

	return GetBytesBySlice(slice), nil
}

func (s *Store) NewIterator(cf string) (*gorocksdb.Iterator, error) {
	cfHandle, ok := s.cfMap[cf]
	if !ok {
		return nil, errors.New("can not find cf:" + cf)
	}

	return s.db.NewIteratorCF(readOpt, cfHandle), nil
}

func (s *Store) GetUint64(cf string, key []byte) (uint64, error) {
	cfHandle, ok := s.cfMap[cf]
	if !ok {
		return 0, errors.New("can not find cf:" + cf)
	}
	slice, err := s.db.GetCF(readOpt, cfHandle, key)
	if err != nil {
		return 0, err
	}

	return getSizeBySlice(slice), nil
}

func (s *Store) Write(wb *gorocksdb.WriteBatch) error {
	return s.db.Write(writeOpt, wb)
}

func (s *Store) GetCfHandle(cf string) (*gorocksdb.ColumnFamilyHandle, error) {
	handle, ok := s.cfMap[cf]
	if !ok {
		return nil, errors.New("can not find cf:" + cf)
	}
	return handle, nil

}

func (s *Store) GetIterator(cfHandle *gorocksdb.ColumnFamilyHandle) *gorocksdb.Iterator {
	return s.db.NewIteratorCF(readOpt, cfHandle)
}

func (s *Store) NewSnapshotDir() (string, error) {
	cp, err := s.db.NewCheckpoint()
	if err != nil {
		return "", err
	}
	defer cp.Destroy()

	// when dragonboat generate snapshot the storage diretory's parent diretory must be exist
	// but the storage diretory must be not exist
	// for example, the snapshot store in /data/raft/data_node_127.0.0.1:9090/1/uuid_123
	// the parent diretory /data/raft/data_node_127.0.0.1:9090/1 must be exist
	if !pathIsExist(s.path) {
		if err := os.MkdirAll(s.path, os.ModePerm); err != nil {
			return "", err
		}
	}

	id := uuid.New().String()
	path := filepath.Join(s.path, id, string(os.PathSeparator))

	err = cp.CreateCheckpoint(path, 0)
	if err != nil {
		return "", err
	}

	return path, nil
}

func (s *Store) SaveSnapShotToWriter(path string, writer io.Writer, stopChan <-chan struct{}) error {
	files := getAllFile(path, path)

	for _, f := range files {
		if isStop(stopChan) {
			return nil
		}

		err := readFileToWriter(path, f, writer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) LoadSnapShotFromReader(reader io.Reader, stopChan <-chan struct{}) error {
	id := uuid.New().String()
	rocksdbPath := filepath.Join(s.path, id)
	dataPath := filepath.Join(rocksdbPath, "data")
	newReader := bufio.NewReaderSize(reader, 4*1024*1024)

	for {
		if isStop(stopChan) {
			return nil
		}

		fileName, err := newReader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}

		fileName = strings.Trim(fileName, "\n")
		fileSizeByte := make([]byte, 8)
		_, err = io.ReadFull(newReader, fileSizeByte)
		if err != nil {
			return err
		}

		fileSize := binary.BigEndian.Uint64(fileSizeByte)
		filePath := filepath.Join(dataPath, fileName)
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}

		file, err := os.Create(filePath)
		if err != nil {
			return err
		}

		_, err = io.CopyN(file, newReader, int64(fileSize))
		_ = file.Close()

		if err != nil {
			return err
		}
	}

	if err := s.Close(); err != nil {
		return err
	}

	currentPath := filepath.Join(s.path, "current")

	if err := os.RemoveAll(currentPath); err != nil {
		return err
	}
	if err := os.Rename(rocksdbPath, currentPath); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join(currentPath, "wal"), os.ModePerm); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join(currentPath, "log"), os.ModePerm); err != nil {
		return err
	}

	db, cfMap, err := createOrOpenRocksDB(currentPath, s.cfs)
	if err != nil {
		return err
	}

	s.db = db
	s.cfMap = cfMap
	return nil
}

func (s *Store) Close() error {
	opt := gorocksdb.NewDefaultFlushOptions()
	opt.SetWait(true)
	if err := s.db.Flush(opt); err != nil {
		return err
	}
	opt.Destroy()

	for _, cf := range s.cfMap {
		if cf != nil {
			cf.Destroy()
		}
	}

	s.cfMap = nil
	if s.db != nil {
		s.db.Close()
		s.db = nil
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

func getSizeBySlice(slice *gorocksdb.Slice) uint64 {
	if slice == nil {
		return 0
	}
	defer slice.Free()
	if !slice.Exists() || slice.Size() != 8 {
		return 0
	} else {
		return binary.BigEndian.Uint64(slice.Data())
	}
}

func GetBytesBySlice(slice *gorocksdb.Slice) []byte {
	if slice == nil {
		return nil
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil
	}
	data := make([]byte, slice.Size())
	copy(data, slice.Data())
	return data

}

func getAllFile(basepath, pathname string) []*FileInfo {
	fileSlice := []*FileInfo{}

	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		panic("read directory error")
	}

	for _, fi := range rd {
		if fi.IsDir() {
			slice := getAllFile(basepath, pathname+"/"+fi.Name())
			fileSlice = append(fileSlice, slice...)
		} else {
			info := &FileInfo{
				FullName: strings.TrimPrefix(filepath.Join(pathname, fi.Name()), basepath),
				Size:     fi.Size(),
			}
			fileSlice = append(fileSlice, info)
		}
	}
	return fileSlice
}

type FileInfo struct {
	FullName string
	Size     int64
}

func readFileToWriter(path string, f *FileInfo, writer io.Writer) error {
	//打开文件
	file, err := os.Open(filepath.Join(path, f.FullName))
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}

	//读取文件名
	var fileName = []byte(f.FullName)
	fileName = append(fileName, '\n')
	if err := writeTo(fileName, writer); err != nil {
		return err
	}
	//读取文件大小
	fileSize := make([]byte, 8)
	binary.BigEndian.PutUint64(fileSize, uint64(f.Size))
	if err := writeTo(fileName, writer); err != nil {
		return err
	}
	//读取文件内容，写入writer
	if _, err := io.Copy(writer, file); err != nil {
		return err
	}
	return nil
}

func writeTo(bytes []byte, writer io.Writer) error {
	size := len(bytes)
	writeSize := 0
	for {
		if size-writeSize == 0 {
			return nil
		}
		n, err := writer.Write(bytes[writeSize:])
		if err != nil {
			return err
		}
		writeSize += n
	}
}

func pathIsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return true
}

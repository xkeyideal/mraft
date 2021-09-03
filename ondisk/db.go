package ondisk

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	mraftDBDirName     string = "/Users/xkey/raftlab/mraft-rocksdb"
	currentDBFilename  string = "current"
	updatingDBFilename string = "current.updating"
)

func isNewRun(dir string) bool {
	fp := filepath.Join(dir, currentDBFilename)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		return true
	}
	return false
}

func getNodeDBDirName(clusterID uint64, nodeID uint64) string {
	return filepath.Join(mraftDBDirName, fmt.Sprintf("%d_%d", clusterID, nodeID))
}

func getNewRandomDBDirName(dir string) string {
	part := "%d_%d"
	rn := rand.Uint64()
	ct := time.Now().UnixNano()
	return filepath.Join(dir, fmt.Sprintf(part, rn, ct))
}

func createNodeDataDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

func getCurrentDBDirName(dir string) (string, error) {
	fp := filepath.Join(dir, currentDBFilename)
	f, err := os.OpenFile(fp, os.O_RDONLY, 0755)
	if err != nil {
		return "", err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	if len(data) <= 8 {
		return "", errors.New("corrupted content")
	}
	crc := data[:8]
	content := data[8:]
	h := md5.New()
	if _, err := h.Write(content); err != nil {
		return "", err
	}
	if !bytes.Equal(crc, h.Sum(nil)[:8]) {
		return "", errors.New("corrupted content with not matched crc")
	}
	return string(content), nil
}

func cleanupNodeDataDir(dir string) error {
	os.RemoveAll(filepath.Join(dir, updatingDBFilename))

	dbdir, err := getCurrentDBDirName(dir)
	if err != nil {
		return err
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}
		//fmt.Printf("dbdir %s, fi.name %s, dir %s\n", dbdir, fi.Name(), dir)
		toDelete := filepath.Join(dir, fi.Name())
		if toDelete != dbdir {
			//fmt.Printf("removing %s\n", toDelete)
			if err := os.RemoveAll(toDelete); err != nil {
				return err
			}
		}
	}

	return nil
}

func replaceCurrentDBFile(dir string) error {
	fp := filepath.Join(dir, currentDBFilename)
	tmpFp := filepath.Join(dir, updatingDBFilename)
	if err := os.Rename(tmpFp, fp); err != nil {
		return err
	}
	return SyncDir(dir)
}

func saveCurrentDBDirName(dir string, dbdir string) error {
	h := md5.New()
	if _, err := h.Write([]byte(dbdir)); err != nil {
		return err
	}

	fp := filepath.Join(dir, updatingDBFilename)
	f, err := os.Create(fp)
	if err != nil {
		return err
	}

	defer func() {
		f.Close()
		SyncDir(dir)
	}()

	if _, err := f.Write(h.Sum(nil)[:8]); err != nil {
		return err
	}
	if _, err := f.Write([]byte(dbdir)); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}

const (
	// DefaultFileMode is the default file mode for files generated by
	// Dragonboat.
	DefaultFileMode    = 0640
	defaultDirFileMode = 0750
	deleteFilename     = "DELETED.dragonboat"
)

// Exist returns whether the specified filesystem entry exists.
func Exist(name string) (bool, error) {
	_, err := os.Stat(name)
	if err != nil && os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// MkdirAll creates the specified dir along with any necessary parents.
func MkdirAll(dir string) error {
	exist, err := Exist(dir)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	parent := filepath.Dir(dir)
	exist, err = Exist(parent)
	if err != nil {
		return err
	}
	if !exist {
		if err := MkdirAll(parent); err != nil {
			return err
		}
	}
	return Mkdir(dir)
}

// Mkdir creates the specified dir.
func Mkdir(dir string) error {
	if err := os.Mkdir(dir, defaultDirFileMode); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(dir))
}

// SyncDir calls fsync on the specified directory.
func SyncDir(dir string) (err error) {
	if runtime.GOOS == "windows" {
		return nil
	}
	fileInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		panic("not a dir")
	}
	df, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer func() {
		if cerr := df.Close(); err == nil {
			err = cerr
		}
	}()
	return df.Sync()
}

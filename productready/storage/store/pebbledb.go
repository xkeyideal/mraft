package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

type PebbleClusterOption struct {
	Target    string
	NodeId    uint64
	ClusterId uint64
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

type pebbleLogger struct {
	fields []zap.Field
	log    *zap.Logger
}

var _ pebble.Logger = (*pebbleLogger)(nil)

func (pl pebbleLogger) Infof(format string, args ...interface{}) {
	pl.log.Info("[raftstorage] [pebbledb] [systemlog]",
		append(pl.fields,
			zap.String("msg", fmt.Sprintf(format, args...)),
		)...,
	)
}

func (pl pebbleLogger) Fatalf(format string, args ...interface{}) {
	pl.log.Error("[raftstorage] [pebbledb] [systemlog]",
		append(pl.fields,
			zap.String("msg", fmt.Sprintf(format, args...)),
		)...,
	)
}

func openPebbleDB(config PebbleDBConfig, dir string, popts PebbleClusterOption, log *zap.Logger) (*pebble.DB, error) {
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

	fields := []zap.Field{
		zap.String("target", popts.Target),
		zap.Uint64("nodeId", popts.NodeId),
		zap.Uint64("clusterId", popts.ClusterId),
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
		Logger:                      pebbleLogger{fields: fields, log: log},
		MaxOpenFiles:                config.KVMaxOpenFiles,
		MaxConcurrentCompactions:    config.KVMaxConcurrentCompactions,
		WALBytesPerSync:             config.KVWALBytesPerSync,
	}

	event := &eventListener{
		opts:   popts,
		log:    log,
		fields: fields,
	}

	opts.EventListener = pebble.EventListener{
		BackgroundError:  event.BackgroundError,
		CompactionBegin:  event.CompactionBegin,
		CompactionEnd:    event.CompactionEnd,
		DiskSlow:         event.DiskSlow,
		FlushBegin:       event.FlushBegin,
		FlushEnd:         event.FlushEnd,
		ManifestCreated:  event.ManifestCreated,
		ManifestDeleted:  event.ManifestDeleted,
		TableCreated:     event.TableCreated,
		TableDeleted:     event.TableDeleted,
		TableIngested:    event.TableIngested,
		TableStatsLoaded: event.TableStatsLoaded,
		WALCreated:       event.WALCreated,
		WALDeleted:       event.WALDeleted,
		WriteStallBegin:  event.WriteStallBegin,
		WriteStallEnd:    event.WriteStallEnd,
	}

	db, err := pebble.Open(dataPath, opts)
	if err != nil {
		return nil, err
	}
	cache.Unref()

	return db, nil
}

type eventListener struct {
	log    *zap.Logger
	opts   PebbleClusterOption
	fields []zap.Field
}

// BackgroundError is invoked whenever an error occurs during a background
// operation such as flush or compaction.
func (l *eventListener) BackgroundError(err error) {
	l.log.Error("[raftstorage] [pebbledb] [BackgroundError]", append(l.fields, zap.Error(err))...)
}

// CompactionBegin is invoked after the inputs to a compaction have been
// determined, but before the compaction has produced any output.
func (l *eventListener) CompactionBegin(info pebble.CompactionInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [CompactionBegin]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [CompactionBegin]", append(l.fields, zap.String("info", msg))...)
	}
}

// CompactionEnd is invoked after a compaction has completed and the result
// has been installed.
func (l *eventListener) CompactionEnd(info pebble.CompactionInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [CompactionEnd]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [CompactionEnd]", append(l.fields, zap.String("info", msg))...)
	}
}

// DiskSlow is invoked after a disk write operation on a file created
// with a disk health checking vfs.FS (see vfs.DefaultWithDiskHealthChecks)
// is observed to exceed the specified disk slowness threshold duration.
func (l *eventListener) DiskSlow(info pebble.DiskSlowInfo) {
	l.log.Error("[raftstorage] [pebbledb] [DiskSlow]", append(l.fields, zap.String("info", info.String()))...)
}

// FlushBegin is invoked after the inputs to a flush have been determined,
// but before the flush has produced any output.
func (l *eventListener) FlushBegin(info pebble.FlushInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [FlushBegin]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [FlushBegin]", append(l.fields, zap.String("info", msg))...)
	}
}

// FlushEnd is invoked after a flush has complated and the result has been
// installed.
func (l *eventListener) FlushEnd(info pebble.FlushInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [FlushEnd]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [FlushEnd]", append(l.fields, zap.String("info", msg))...)
	}
}

// ManifestCreated is invoked after a manifest has been created.
func (l *eventListener) ManifestCreated(info pebble.ManifestCreateInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [ManifestCreated]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [ManifestCreated]", append(l.fields, zap.String("info", msg))...)
	}
}

// ManifestDeleted is invoked after a manifest has been deleted.
func (l *eventListener) ManifestDeleted(info pebble.ManifestDeleteInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [ManifestDeleted]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [ManifestDeleted]", append(l.fields, zap.String("info", msg))...)
	}
}

// TableCreated is invoked when a table has been created.
func (l *eventListener) TableCreated(info pebble.TableCreateInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	l.log.Info("[raftstorage] [pebbledb] [TableCreated]", append(l.fields, zap.String("info", msg))...)
}

// TableDeleted is invoked after a table has been deleted.
func (l *eventListener) TableDeleted(info pebble.TableDeleteInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [TableDeleted]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [TableDeleted]", append(l.fields, zap.String("info", msg))...)
	}
}

// TableIngested is invoked after an externally created table has been
// ingested via a call to DB.Ingest().
func (l *eventListener) TableIngested(info pebble.TableIngestInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [TableIngested]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [TableIngested]", append(l.fields, zap.String("info", msg))...)
	}
}

// TableStatsLoaded is invoked at most once, when the table stats
// collector has loaded statistics for all tables that existed at Open.
func (l *eventListener) TableStatsLoaded(info pebble.TableStatsInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	l.log.Info("[raftstorage] [pebbledb] [TableStatsLoaded]", append(l.fields, zap.String("info", msg))...)
}

// WALCreated is invoked after a WAL has been created.
func (l *eventListener) WALCreated(info pebble.WALCreateInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [WALCreated]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [WALCreated]", append(l.fields, zap.String("info", msg))...)
	}
}

// WALDeleted is invoked after a WAL has been deleted.
func (l *eventListener) WALDeleted(info pebble.WALDeleteInfo) {
	msg := strings.ReplaceAll(info.String(), "[", "(")
	msg = strings.ReplaceAll(msg, "]", ")")
	if info.Err != nil {
		l.log.Error("[raftstorage] [pebbledb] [WALDeleted]", append(l.fields, zap.String("info", msg))...)
	} else {
		l.log.Info("[raftstorage] [pebbledb] [WALDeleted]", append(l.fields, zap.String("info", msg))...)
	}
}

// WriteStallBegin is invoked when writes are intentionally delayed.
func (l *eventListener) WriteStallBegin(info pebble.WriteStallBeginInfo) {
	l.log.Warn("[raftstorage] [pebbledb] [WriteStallBegin]", append(l.fields, zap.String("info", info.String()))...)
}

// WriteStallEnd is invoked when delayed writes are released.
func (l *eventListener) WriteStallEnd() {
	l.log.Warn("[raftstorage] [pebbledb] [WriteStallEnd]", l.fields...)
}

package storage

import (
	"os"
	"path/filepath"

	"github.com/tecbot/gorocksdb"
)

var (
	readOpt  = gorocksdb.NewDefaultReadOptions()
	writeOpt = gorocksdb.NewDefaultWriteOptions()
	env      *gorocksdb.Env
	bbto     *gorocksdb.BlockBasedTableOptions
)

type RocksDBConfig struct {
	HighPriorityThreads            int
	LowPriorityThreads             int
	WriteBufferSize                int
	WriteBufferNumber              int
	WriteBufferNumberToMerge       int
	LRUSize                        uint64
	BlockSize                      int
	WALTtlSeconds                  uint64
	WalSizeLimitMb                 uint64
	MaxTotalWalSize                uint64
	Level0FileNumCompactionTrigger int
	MaxBytesForLevelBase           uint64
	MaxBackgroundJobs              int
	Level0StopWritesTrigger        int
	Level0SlowdownWritesTrigger    int
	MaxSubcompactions              uint32
	BytesPerSync                   uint64
	MaxLogFileSize                 int
	KeepLogFileNum                 int
	PeriodicCompactionSeconds      int
}

var defaultRocksDBConfig *RocksDBConfig

func init() {
	defaultRocksDBConfig = &RocksDBConfig{
		HighPriorityThreads:            8,
		LowPriorityThreads:             16,
		WriteBufferSize:                128 * 1024 * 1024, //128M
		WriteBufferNumber:              32,
		WriteBufferNumberToMerge:       4,
		LRUSize:                        8 * 1024 * 1024 * 1024, //8G
		BlockSize:                      64 * 1024,              //64K
		WALTtlSeconds:                  3600,                   //1小时
		WalSizeLimitMb:                 1024 * 1024 * 1024,     //1G
		MaxTotalWalSize:                256 * 1024 * 1024,      //256M
		Level0FileNumCompactionTrigger: 4,
		MaxBytesForLevelBase:           1024 * 1024 * 1024, //1G
		MaxBackgroundJobs:              8,
		Level0StopWritesTrigger:        16,
		Level0SlowdownWritesTrigger:    24,
		MaxSubcompactions:              8,
		BytesPerSync:                   1024 * 1024,
		MaxLogFileSize:                 128 * 1024 * 1024,
		KeepLogFileNum:                 16,
		PeriodicCompactionSeconds:      43200,
	}
}

func GetRocksDBConfig() *RocksDBConfig {
	return defaultRocksDBConfig
}

func init() {
	cfg := GetRocksDBConfig()
	env = gorocksdb.NewDefaultEnv()
	env.SetBackgroundThreads(cfg.LowPriorityThreads)
	env.SetHighPriorityBackgroundThreads(cfg.HighPriorityThreads)
	bbto = buildLRUCacheOption(cfg.LRUSize, cfg.BlockSize)
	readOpt = gorocksdb.NewDefaultReadOptions()
	writeOpt = gorocksdb.NewDefaultWriteOptions()
	writeOpt.SetSync(false)
}

func buildLRUCacheOption(LRUSize uint64, blockSize int) *gorocksdb.BlockBasedTableOptions {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	//设置LRUCache的大小
	var cache = gorocksdb.NewLRUCache(LRUSize)
	//var cache = gorocksdb.NewLRUCacheFull(LRUSize, -1, false, 0.6)
	bbto.SetBlockCache(cache)
	//设置数据块的大小，每次rocksdb从磁盘上读取数据到cache里面，以及从cache里面淘汰数据都是以这个块大小为单位
	//设置当前的数据块的大小，默认是4K，当前程序以区间扫描为主，所以设置为64K，每次磁盘IO交互64K数据
	bbto.SetBlockSize(blockSize)
	//是否缓存索引数据和筛选数据,将索引和筛选数据放在内存里面可以有效提高查询效率
	bbto.SetCacheIndexAndFilterBlocks(true)
	bbto.SetPinTopLevelIndexAndFilter(true)
	//是否将索引数据和筛选数据始终放置在L0,这样查询效率会高很多
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)
	//将索引数据和筛选数据放在cache的高优先级队列里面
	bbto.SetCacheIndexAndFilterBlocksWithHighPriority(true)
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)

	//可以设置BlockBasedTableOptions.enable_index_compression设置为false来关闭索引的压缩--没找到参数
	//设置bloom过滤器
	bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(10))

	//bbto.SetPartitionFilters(true)q
	//bbto.SetMetadataBlockSize(4096)
	//
	////启用分区索引
	//bbto.SetIndexType(gorocksdb.KTwoLevelIndexSearchIndexType)
	return bbto
}

func getRocksdbOpts(walDir, logDir string) (*gorocksdb.Options, error) {
	var config = GetRocksDBConfig()
	opts := gorocksdb.NewDefaultOptions()
	//设置前16位是bloom过滤器--暂时先不设置，先看看性能如何
	//opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(16))
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	//设置后台合并线程数量，高优先级8个，低优先级16个
	opts.SetEnv(env)

	//设置n-1层的压缩方式是LZ4
	opts.SetCompression(gorocksdb.LZ4Compression)
	//没有找到最后一层的压缩算法，想设置为ZSTD
	//opts.SetBottommostCompression(gorocksdb.ZSTDCompression)

	opts.SetDbLogDir(logDir)
	opts.SetWalDir(walDir)

	opts.SetWriteBufferSize(config.WriteBufferSize)
	opts.SetMaxWriteBufferNumber(config.WriteBufferNumber)
	//设置写盘时合并的写缓冲区块的数量，假设设置为2，那么等到内存中有2个MemTable需要落盘时，会合并这2个memtable，然后保存
	opts.SetMinWriteBufferNumberToMerge(config.WriteBufferNumberToMerge)

	opts.SetBlockBasedTableFactory(bbto)

	//启用此选项后，我们将不会在包含数据库90％的最后一个级别上构建布隆过滤器。因此，布隆过滤器的内存使用量将减少10倍。不过，您将为每个在数据库中找不到数据的Get（）支付一个IO
	//opts.SetOptimizeFiltersForHits(true)

	//设置wal日志为保留7天或超过了64G,wal文件的最大大小为512M
	opts.SetWALTtlSeconds(config.WALTtlSeconds)
	opts.SetWalSizeLimitMb(config.WalSizeLimitMb)

	//opts.SetRecycleLogFileNum(256)

	opts.SetMaxTotalWalSize(config.MaxTotalWalSize)

	//设置L0有4个文件就合并
	opts.SetLevel0FileNumCompactionTrigger(config.Level0FileNumCompactionTrigger)
	//设置基础层级的大小
	opts.SetMaxBytesForLevelBase(config.MaxBytesForLevelBase)
	//periodic_compaction_seconds--设置SST文件的合并时间，到时间后就自动合并
	//之前为了回收过期的数据加入了这个功能，现在每个key在保存时会记录自己的过期时间，然后垃圾回收器会定期扫描，这个功能暂时不用了，先注释掉
	//opts.SetPeriodicCompactionSeconds(uint64(cfg.PeriodicCompactionSeconds))
	//设置为动态合并
	opts.SetLevelCompactionDynamicLevelBytes(true)
	//设置最大同时进行的压缩数
	opts.SetMaxBackgroundCompactions(config.MaxBackgroundJobs)
	opts.SetLevel0FileNumCompactionTrigger(config.Level0FileNumCompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(config.Level0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(config.Level0StopWritesTrigger)
	//L0不能并行压缩，所以可以设置max_subcompactions进行数据的分片压缩
	//opts.SetMaxSubcompactions(config.MaxSubcompactions)
	//每层的放大倍数
	opts.SetMaxBytesForLevelMultiplier(10)
	//rocksdb设置为8层
	//opts.SetNumLevels(8)
	//当SST文件变更达到1M，调用Linux的sync_file_range,将数据写回磁盘
	opts.SetBytesPerSync(config.BytesPerSync)

	//设置日志文件的大小和日志文件的数量
	opts.SetMaxLogFileSize(config.MaxLogFileSize)
	opts.SetKeepLogFileNum(config.KeepLogFileNum)
	opts.SetInfoLogLevel(gorocksdb.ErrorInfoLogLevel) // rocksdb日志级别

	opts.SetMaxOpenFiles(-1)

	return opts, nil
}

// dbPath:base/data_node_nodeId/clusterId/current
func createOrOpenRocksDB(dbPath string, cfs []string) (*gorocksdb.DB, map[string]*gorocksdb.ColumnFamilyHandle, error) {
	cfs = append(cfs, "default")

	// base/data_node_nodeId/clusterId/current/data
	dataPath := filepath.Join(dbPath, "data")
	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		return nil, nil, err
	}

	// base/data_node_nodeId/clusterId/current/wal
	walPath := filepath.Join(dbPath, "wal")
	if err := os.MkdirAll(walPath, os.ModePerm); err != nil {
		return nil, nil, err
	}

	// base/data_node_nodeId/clusterId/current/log
	logPath := filepath.Join(dbPath, "log")
	if err := os.MkdirAll(logPath, os.ModePerm); err != nil {
		return nil, nil, err
	}

	opts, err := getRocksdbOpts(walPath, logPath)
	if err != nil {
		return nil, nil, err
	}

	cfOpts := []*gorocksdb.Options{}
	for range cfs {
		cfOpts = append(cfOpts, opts)
	}

	tmpDb, cfList, err := gorocksdb.OpenDbColumnFamilies(opts, dataPath, cfs, cfOpts)
	if err != nil {
		return nil, nil, err
	}

	cfMap := make(map[string]*gorocksdb.ColumnFamilyHandle)
	for i := range cfs {
		cfMap[cfs[i]] = cfList[i]
	}

	return tmpDb, cfMap, nil
}

func openReadonlyRocksDB(dbPath string, cfs []string) (*gorocksdb.DB, map[string]*gorocksdb.ColumnFamilyHandle, error) {
	cfs = append(cfs, "default")

	// base/data_node_nodeId/clusterId/current/wal
	walPath := filepath.Join(dbPath, "wal")
	if err := os.MkdirAll(walPath, os.ModePerm); err != nil {
		return nil, nil, err
	}

	// base/data_node_nodeId/clusterId/current/log
	logPath := filepath.Join(dbPath, "log")
	if err := os.MkdirAll(logPath, os.ModePerm); err != nil {
		return nil, nil, err
	}

	opts, err := getRocksdbOpts(walPath, logPath)
	if err != nil {
		return nil, nil, err
	}

	cfOpts := []*gorocksdb.Options{}
	for range cfs {
		cfOpts = append(cfOpts, opts)
	}

	dbDir := filepath.Join(dbPath, "data")
	tmpDb, cfList, err := gorocksdb.OpenDbForReadOnlyColumnFamilies(opts, dbDir, cfs, cfOpts, false)
	if err != nil {
		return nil, nil, err
	}

	cfMap := make(map[string]*gorocksdb.ColumnFamilyHandle)
	for i := range cfs {
		cfMap[cfs[i]] = cfList[i]
	}

	return tmpDb, cfMap, nil
}

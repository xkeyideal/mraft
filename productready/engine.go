package productready

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/xkeyideal/mraft/productready/config"
	"github.com/xkeyideal/mraft/productready/httpd"
	"github.com/xkeyideal/mraft/productready/storage"
	"go.uber.org/zap/zapcore"

	"github.com/gin-gonic/gin"
)

type Engine struct {
	prefix string

	server *http.Server
	router *gin.Engine

	raftStorage *storage.Storage

	kvHandle *httpd.KVHandle
}

var (
	clusterIds = []uint64{0, 1, 2}
)

func NewEngine(cfg *config.DynamicConfig) *Engine {
	raftCfg := &storage.RaftConfig{
		LogDir:         cfg.LogDir,
		LogLevel:       zapcore.DebugLevel,
		HostIP:         cfg.IP,
		NodeId:         cfg.NodeId,
		ClusterIds:     clusterIds,
		RaftAddr:       fmt.Sprintf("%s:%d", cfg.IP, cfg.RaftPort),
		MultiGroupSize: uint32(len(clusterIds)),
		StorageDir:     cfg.RaftDir,
		Join:           cfg.Join,
		InitialMembers: cfg.InitialMembers,
		// Gossip:         metadata.Gossip,
		// GossipPort:     metadata.GossipPort,
		// GossipSeeds:    metadata.GossipSeeds,
		Metrics: false,
		// BindAddress: fmt.Sprintf("%s:%d", engine.cfg.IP, metadata.GossipConfig.BindPort),
		// BindPort:    uint16(metadata.GossipConfig.BindPort),
		// Seeds:       metadata.GossipConfig.Seeds,
	}

	raftStorage, err := storage.NewStorage(raftCfg)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("raft started, waiting raft cluster ready")

	// 等待raft集群ready
	err = raftStorage.RaftReady()
	if err != nil {
		log.Fatalf("[ERROR] raft ready %s\n", err.Error())
	}

	router := gin.New()
	router.Use(gin.Recovery())

	engine := &Engine{
		prefix: "/raft",
		router: router,
		server: &http.Server{
			Addr:         fmt.Sprintf("0.0.0.0:%s", cfg.HttpPort),
			Handler:      router,
			ReadTimeout:  20 * time.Second,
			WriteTimeout: 40 * time.Second,
		},
		raftStorage: raftStorage,
		kvHandle:    httpd.NewKVHandle("kvstorage", raftStorage),
	}

	engine.registerRouter(router)

	go func() {
		if err := engine.server.ListenAndServe(); err != nil {
			panic(err.Error())
		}
	}()

	return engine
}

func (engine *Engine) Stop() {
	if engine.server != nil {
		if err := engine.server.Shutdown(context.Background()); err != nil {
			fmt.Println("Server Shutdown: ", err)
		}
	}

	engine.raftStorage.StopRaftNode()
}

package engine

import (
	"context"
	"fmt"
	"log"
	"mraft/config"
	"mraft/ondisk"
	"mraft/raftd"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Engine struct {
	prefix string

	nodeID      uint64
	raftDataDir string

	server *http.Server
	router *gin.Engine

	nh *ondisk.OnDiskRaft

	mraftHandle *raftd.MRaftHandle
}

func NewEngine(nodeID uint64, port string) *Engine {

	cfg := config.NewOnDiskRaftConfig()

	router := gin.New()
	router.Use(gin.Recovery())

	var nh *ondisk.OnDiskRaft
	if nodeID == 10003 || nodeID == 10004 || nodeID == 5 {
		nh = ondisk.NewOnDiskRaft(map[uint64]string{}, cfg.RaftClusterIDs)
	} else {
		nh = ondisk.NewOnDiskRaft(cfg.RaftNodePeers, cfg.RaftClusterIDs)
	}

	engine := &Engine{
		nodeID:      nodeID,
		raftDataDir: cfg.RaftDataDir,
		prefix:      "/mraft",
		router:      router,
		server: &http.Server{
			Addr:         fmt.Sprintf("0.0.0.0:%s", port), //"9080"
			Handler:      router,
			ReadTimeout:  20 * time.Second,
			WriteTimeout: 40 * time.Second,
		},
		nh:          nh,
		mraftHandle: raftd.NewMRaftHandle(nh),
	}

	engine.registerMraftRouter(router)

	return engine
}

func (engine *Engine) Start() {
	join := false
	nodeAddr := ""
	if engine.nodeID == 10003 {
		join = true
		nodeAddr = "10.181.20.34:11300"
	} else if engine.nodeID == 10004 {
		join = true
		nodeAddr = "10.181.20.34:11400"
	} else if engine.nodeID == 10005 {
		join = true
		nodeAddr = "10.181.20.34:11500"
	}

	engine.nh.Start(engine.raftDataDir, engine.nodeID, nodeAddr, join)

	// 等待raft集群ready
	for {
		if engine.nh.ClusterAllReady() {
			break
		}
		time.Sleep(2 * time.Second)
	}

	log.Println("cluster all ready")

	if err := engine.server.ListenAndServe(); err != nil {
		panic(err.Error())
	}
}

func (engine *Engine) Stop() {
	if engine.server != nil {
		if err := engine.server.Shutdown(context.Background()); err != nil {
			fmt.Println("Server Shutdown: ", err)
		}
	}

	engine.nh.Stop()
}

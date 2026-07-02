package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/xkeyideal/mraft/experiment/ondisk"
	"github.com/xkeyideal/mraft/experiment/ondisk/raftd"

	"github.com/gin-gonic/gin"
)

type Engine struct {
	prefix string

	nodeID      uint64
	raftDataDir string
	raftAddr    string

	server *http.Server
	router *gin.Engine

	nh *ondisk.OnDiskRaft

	mraftHandle *raftd.MRaftHandle
}

var clusterIDs = []uint64{14000, 14100, 14200}

func isJoinNode(nodeID uint64) bool {
	return nodeID == 10003 || nodeID == 10004 || nodeID == 10005
}

func NewEngine(nodeID uint64, httpPort int, raftDataDir, raftAddr string, initialPeers map[uint64]string) *Engine {

	// 状态机与 dragonboat NodeHost 共享同一个根目录，子目录不同
	ondisk.SetDBDir(raftDataDir)

	router := gin.New()
	router.Use(gin.Recovery())

	var nh *ondisk.OnDiskRaft
	if isJoinNode(nodeID) {
		nh = ondisk.NewOnDiskRaft(map[uint64]string{}, clusterIDs)
	} else {
		nh = ondisk.NewOnDiskRaft(initialPeers, clusterIDs)
	}

	engine := &Engine{
		nodeID:      nodeID,
		raftDataDir: raftDataDir,
		raftAddr:    raftAddr,
		prefix:      "/mraft",
		router:      router,
		server: &http.Server{
			Addr:         fmt.Sprintf("0.0.0.0:%d", httpPort),
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
	join := isJoinNode(engine.nodeID)
	nodeAddr := ""
	if join {
		nodeAddr = engine.raftAddr
	}

	if err := engine.nh.Start(engine.raftDataDir, engine.nodeID, nodeAddr, join); err != nil {
		log.Fatalf("start raft failed: %v", err)
	}

	// 等待raft集群ready
	for {
		if engine.nh.ClusterAllReady() {
			break
		}
		time.Sleep(2 * time.Second)
	}

	log.Println("cluster all ready")

	if err := engine.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err.Error())
	}
}

func (engine *Engine) Stop() {
	if engine.server != nil {
		if err := engine.server.Shutdown(context.Background()); err != nil {
			fmt.Println("Server Shutdown: ", err)
		}
	}

	if engine.nh != nil {
		engine.nh.Stop()
	}
}

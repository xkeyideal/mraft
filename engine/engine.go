package engine

import (
	"context"
	"fmt"
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

	nh := ondisk.NewOnDiskRaft(cfg.RaftNodePeers, cfg.RaftClusterIDs)

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
	go engine.nh.Start(engine.raftDataDir, engine.nodeID)

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

package engine

import (
	"context"
	"fmt"
	"mraft/ondisk"
	"mraft/raftd"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Engine struct {
	prefix string

	nodeID uint64

	server *http.Server
	router *gin.Engine

	nh *ondisk.OnDiskRaft

	mraftHandle *raftd.MRaftHandle
}

func NewEngine(nodeID uint64, port string) *Engine {
	router := gin.New()
	router.Use(gin.Recovery())

	peers := map[uint64]string{
		10000: "10.101.44.4:54000",
		10001: "10.101.44.4:54100",
		10002: "10.101.44.4:54200",
	}

	clusters := []uint64{254000, 254100, 254200}

	nh := ondisk.NewOnDiskRaft(peers, clusters)

	engine := &Engine{
		nodeID: nodeID,
		prefix: "/mraft",
		router: router,
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
	go engine.nh.Start(engine.nodeID)

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

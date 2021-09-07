package productready

import (
	"context"
	"fmt"
	"log"
	"mraft/productready/config"
	"mraft/productready/httpd"
	"mraft/productready/storage"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Engine struct {
	prefix string

	server *http.Server
	router *gin.Engine

	raftStorage *storage.Storage

	kvHandle *httpd.KVHandle
}

func NewEngine(dcfg *config.DynamicConfig) *Engine {
	cfg := config.NewOnDiskRaftConfig(dcfg)
	if cfg.Join {
		// 先调用webapi的http接口，告知集群有新节点加入
		err := cfg.JoinNewNode(fmt.Sprintf("%s:%d", dcfg.IP, dcfg.RaftPort))
		if err != nil {
			log.Fatal("join new node", err)
		}
	}

	cfs := []string{
		"kvstorage",
	}

	raftStorage, err := storage.NewStorage(
		cfg.DeploymentID,
		int(cfg.NodeID),
		fmt.Sprintf("%s:%s", cfg.IP, cfg.RaftPort),
		cfg.DataDir,
		cfs,
		cfg.Join,
		cfg.Peers,
	)
	if err != nil {
		log.Fatal(err)
	}

	// 等待raft集群ready
	for {
		if raftStorage.ClusterAllReady() {
			break
		}
		time.Sleep(2 * time.Second)
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

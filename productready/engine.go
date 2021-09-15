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
		code, err := cfg.JoinNewNode(fmt.Sprintf("%s:%d", dcfg.IP, dcfg.RaftPort))
		if code < 0 {
			log.Fatal("join new node", err)
		}

		// 新增的节点已经存在, 视为节点重启，根据dragonboat的文档:
		// 当一个节点重启时，不论该节点是一个初始节点还是后续通过成员变更添加的节点，均无需再次提供初始成员信息，也不再需要设置join参数为true
		if code == 1 {
			cfg.Join = false
			cfg.Peers = make(map[uint64]string)
			log.Println("join new node restart")
		}
	}

	cfs := []string{
		"kvstorage",
	}

	raftStorage, err := storage.NewStorage(
		cfg.DeploymentID,
		cfg.NodeID,
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

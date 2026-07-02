package httpengine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xkeyideal/mraft/experiment/simpleondisk"
)

var raftClusterIDs = []uint64{234000, 234100, 234200}

type Engine struct {
	nodeID      uint64
	raftDataDir string

	server *http.Server
	router *gin.Engine

	nh *simpleondisk.SimpleOnDiskRaft
}

func NewEngine(nodeID uint64, httpPort int, raftDataDir, ip string, baseRaftPort uint64) *Engine {

	simpleondisk.SetDBDir(raftDataDir)

	peers := make(map[uint64]string)
	for _, id := range []uint64{10000, 10001, 10002} {
		p := baseRaftPort + (id - 10000)
		peers[id] = fmt.Sprintf("%s:%d", ip, p)
	}

	router := gin.New()
	router.Use(gin.Recovery())

	nh := simpleondisk.NewSimpleOnDiskRaft(peers, raftClusterIDs)

	engine := &Engine{
		nodeID:      nodeID,
		raftDataDir: raftDataDir,
		router:      router,
		server: &http.Server{
			Addr:         fmt.Sprintf("0.0.0.0:%d", httpPort),
			Handler:      router,
			ReadTimeout:  20 * time.Second,
			WriteTimeout: 40 * time.Second,
		},
		nh: nh,
	}

	engine.router.GET("/msimpleraft/key", engine.Query)
	engine.router.POST("/msimpleraft/key", engine.Upsert)

	return engine
}

func (engine *Engine) Start() {

	if err := engine.nh.Start(engine.raftDataDir, engine.nodeID, "", false); err != nil {
		log.Fatalf("start raft failed: %v", err)
	}

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

func (engine *Engine) Query(c *gin.Context) {
	key := c.Query("key")
	hashKey, err := strconv.ParseUint(c.Query("hashKey"), 10, 64)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	val, err := engine.nh.SyncRead(key, hashKey)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}
	SetStrResp(http.StatusOK, 0, "", string(val), c)
}

func (engine *Engine) Upsert(c *gin.Context) {
	key := c.Query("key")
	hashKey, err := strconv.ParseUint(c.Query("hashKey"), 10, 64)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	val, err := strconv.ParseUint(c.Query("val"), 10, 64)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	if err := engine.nh.Write(key, hashKey, int(val)); err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	SetStrResp(http.StatusOK, 0, "", "OK", c)
}

func SetStrResp(httpCode, code int, msg string, result interface{}, c *gin.Context) {

	m := msg

	if code == 0 {
		c.JSON(httpCode, gin.H{
			"code":   code,
			"msg":    m,
			"result": result,
		})
	} else {
		c.JSON(httpCode, gin.H{
			"code": code,
			"msg":  m,
		})
	}
}

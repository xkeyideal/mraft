package httpengine

import (
	"context"
	"fmt"
	"mraft/simpleondisk"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	RaftDataDir   = "/Volumes/ST1000/mraft-simpleondisk"
	RaftNodePeers = map[uint64]string{
		10000: "10.101.44.4:34000",
		10001: "10.101.44.4:34100",
		10002: "10.101.44.4:34200",
	}
	RaftClusterIDs = []uint64{234000, 234100, 234200}
)

type Engine struct {
	nodeID      uint64
	raftDataDir string

	server *http.Server
	router *gin.Engine

	nh *simpleondisk.SimpleOnDiskRaft
}

func NewEngine(nodeID uint64, port string) *Engine {

	router := gin.New()
	router.Use(gin.Recovery())

	nh := simpleondisk.NewSimpleOnDiskRaft(RaftNodePeers, RaftClusterIDs)

	engine := &Engine{
		nodeID:      nodeID,
		raftDataDir: RaftDataDir,
		router:      router,
		server: &http.Server{
			Addr:         fmt.Sprintf("0.0.0.0:%s", port), //"9080"
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

	engine.nh.Start(engine.raftDataDir, engine.nodeID, "", false)

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

	engine.nh.Write(key, hashKey, int(val))

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

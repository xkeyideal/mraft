package httpd

import (
	"context"
	"fmt"
	"mraft/productready/storage"
	"mraft/productready/utils"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type KVHandle struct {
	cf          string
	raftStorage *storage.Storage
}

func NewKVHandle(cf string, raftStorage *storage.Storage) *KVHandle {
	return &KVHandle{
		cf:          cf,
		raftStorage: raftStorage,
	}
}

func (mh *KVHandle) Info(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	info, _ := mh.raftStorage.GetMembership(ctx)
	utils.SetStrResp(http.StatusOK, 0, "", info, c)
}

func (mh *KVHandle) Query(c *gin.Context) {
	key := c.Query("key")
	sync := c.Query("sync")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	val, err := mh.raftStorage.Get(ctx, mh.cf, key, sync == "true", []byte(key))
	if err != nil {
		utils.SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	utils.SetStrResp(http.StatusOK, 0, "", string(val), c)
}

func (mh *KVHandle) Upsert(c *gin.Context) {
	key := c.Query("key")
	val := c.Query("val")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := mh.raftStorage.Put(ctx, mh.cf, key, []byte(key), []byte(val))
	if err != nil {
		utils.SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	utils.SetStrResp(http.StatusOK, 0, "", "OK", c)
}

func (mh *KVHandle) Delete(c *gin.Context) {
	key := c.Query("key")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := mh.raftStorage.Del(ctx, mh.cf, key, []byte(key))
	if err != nil {
		utils.SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	utils.SetStrResp(http.StatusOK, 0, "", "OK", c)
}

func (mh *KVHandle) JoinNode(c *gin.Context) {
	nodeAddr := c.Query("addr")

	raftAddrs := mh.raftStorage.GetNodeHost()
	for _, raftAddr := range raftAddrs {
		if nodeAddr == raftAddr {
			utils.SetStrResp(http.StatusOK, 1, fmt.Sprintf("%s 待加入的节点已经在集群raft节点中", nodeAddr), "OK", c)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := mh.raftStorage.AddRaftNode(ctx, utils.Addr2RaftNodeID(nodeAddr), nodeAddr)
	if err != nil {
		utils.SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}
	utils.SetStrResp(http.StatusOK, 0, "", "OK", c)
}

func (mh *KVHandle) DelNode(c *gin.Context) {
	nodeAddr := c.Query("addr")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := mh.raftStorage.RemoveRaftNode(ctx, utils.Addr2RaftNodeID(nodeAddr))
	if err != nil {
		utils.SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}
	utils.SetStrResp(http.StatusOK, 0, "", "OK", c)
}

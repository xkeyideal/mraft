package raftd

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/xkeyideal/mraft/experiment/ondisk"
	"github.com/xkeyideal/mraft/experiment/store"

	"github.com/gin-gonic/gin"
)

type MRaftHandle struct {
	raft *ondisk.OnDiskRaft
}

func NewMRaftHandle(raft *ondisk.OnDiskRaft) *MRaftHandle {
	return &MRaftHandle{
		raft: raft,
	}
}

func (mh *MRaftHandle) Info(c *gin.Context) {
	SetStrResp(http.StatusOK, 0, "", mh.raft.Info(), c)
}

func (mh *MRaftHandle) Query(c *gin.Context) {
	key := c.Query("key")
	sync := c.Query("sync")
	hashKey, err := strconv.ParseUint(c.Query("hashKey"), 10, 64)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	if sync == "true" {
		val, err := mh.raft.SyncRead(key, hashKey)
		if err != nil {
			SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
			return
		}
		SetStrResp(http.StatusOK, 0, "", val, c)
		return
	}

	val, err := mh.raft.ReadLocal(key, hashKey)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	SetStrResp(http.StatusOK, 0, "", val, c)
}

func (mh *MRaftHandle) Upsert(c *gin.Context) {
	bytes, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	attr := &store.RaftAttribute{}
	err = json.Unmarshal(bytes, attr)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	cmd, err := attr.GenerateCommand(store.CommandUpsert)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	mh.raft.Write(cmd)

	SetStrResp(http.StatusOK, 0, "", "OK", c)
}

func (mh *MRaftHandle) Delete(c *gin.Context) {
	bytes, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	attr := &store.RaftAttribute{}
	err = json.Unmarshal(bytes, attr)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	cmd, err := attr.GenerateCommand(store.CommandDelete)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	mh.raft.Write(cmd)

	SetStrResp(http.StatusOK, 0, "", "OK", c)
}

func (mh *MRaftHandle) JoinNode(c *gin.Context) {
	nodeID, err := strconv.ParseUint(c.Query("nodeID"), 10, 64)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	nodeAddr := c.Query("nodeAddr")

	err = mh.raft.RaftAddNode(nodeID, nodeAddr)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}
	SetStrResp(http.StatusOK, 0, "", "OK", c)
}

func (mh *MRaftHandle) DelNode(c *gin.Context) {
	nodeID, err := strconv.ParseUint(c.Query("nodeID"), 10, 64)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	err = mh.raft.RaftRemoveNode(nodeID)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}
	SetStrResp(http.StatusOK, 0, "", "OK", c)
}

func (mh *MRaftHandle) RaftMetrics(c *gin.Context) {
	SetStrResp(http.StatusOK, 0, "", mh.raft.MetricsInfo(), c)
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

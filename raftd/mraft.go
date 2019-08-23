package raftd

import (
	"encoding/json"
	"io/ioutil"
	"mraft/ondisk"
	"net/http"

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
	val, err := mh.raft.Read(key)
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

	cmd := &ondisk.KvCmd{}
	err = json.Unmarshal(bytes, cmd)
	if err != nil {
		SetStrResp(http.StatusBadRequest, -1, err.Error(), "", c)
		return
	}

	mh.raft.Write(cmd)

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

package config

import (
	"encoding/json"
	"fmt"
	"mraft/productready/utils"
	"strconv"
	"time"

	"github.com/xkeyideal/gokit/httpkit"
)

type DynamicConfig struct {
	// raft数据存储目录
	RaftDir string `json:"raftDir"`

	// raft最初的集群地址,IP+Port
	RaftPeers []string `json:"raftPeers"`

	// 该节点是否是raft的最初集群之一
	Native bool `json:"native"`

	// join 节点时，http join的接口地址
	JoinUrl string `json:"joinUrl"`

	// 本机的地址
	IP string `json:"ip"`

	// raft port
	RaftPort uint16 `json:"raftPort"`
}

type OnDiskRaftConfig struct {
	DataDir      string
	Peers        map[uint64]string
	NodeID       uint64
	Join         bool // 该节点是否以join的方式加入raft集群
	RaftPort     string
	HttpPort     string
	IP           string
	JoinUrl      string
	DeploymentID uint64
}

func NewOnDiskRaftConfig(dcfg *DynamicConfig) *OnDiskRaftConfig {
	if dcfg.IP == "" {
		panic("machine IP required")
	}

	if dcfg.RaftPort == 0 {
		dcfg.RaftPort = 13890
	}

	var nodeID uint64 = 0
	raftPeers := map[uint64]string{}
	if !dcfg.Native {
		id := utils.Addr2RaftNodeID(fmt.Sprintf("%s:%d", dcfg.IP, dcfg.RaftPort))
		nodeID = id
	} else {
		for _, addr := range dcfg.RaftPeers {
			id := utils.Addr2RaftNodeID(addr)
			raftPeers[id] = addr
			if addr == fmt.Sprintf("%s:%d", dcfg.IP, dcfg.RaftPort) {
				nodeID = id
			}
		}
	}

	cfg := &OnDiskRaftConfig{
		DataDir:      dcfg.RaftDir,
		Peers:        raftPeers,
		Join:         !dcfg.Native,
		NodeID:       nodeID,
		RaftPort:     fmt.Sprintf("%d", dcfg.RaftPort),
		HttpPort:     strconv.FormatUint(uint64((dcfg.RaftPort%65535)+1), 10),
		IP:           dcfg.IP,
		JoinUrl:      dcfg.JoinUrl,
		DeploymentID: 27,
	}

	return cfg
}

func (cfg *OnDiskRaftConfig) JoinNewNode(addr string) (int, error) {
	client := httpkit.NewHttpClient(2*time.Second, 1, 2*time.Second, 3*time.Second, nil)

	client = client.SetParams(map[string]string{
		"addr": addr,
	})

	resp, err := client.Get(cfg.JoinUrl)
	if err != nil {
		return -1, err
	}

	if resp.StatusCode != 200 && resp.StatusCode != 400 {
		return -1, fmt.Errorf("Join Node http request failed httpcode:%d", resp.StatusCode)
	}

	respBody := struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}{}

	err = json.Unmarshal(resp.Body, &respBody)
	if err != nil {
		return -1, err
	}

	if respBody.Code == 0 {
		return 0, nil
	}

	if respBody.Code == 1 {
		return 1, nil
	}

	return -1, fmt.Errorf("Join Node failed code:%d, msg:%s", respBody.Code, respBody.Msg)
}

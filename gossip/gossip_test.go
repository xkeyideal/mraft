package gossip

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"go.uber.org/zap/zapcore"
)

func WriteClusterToFile(cluster *RaftClusterMessage) error {
	return nil
}

var (
	logDir = "/tmp/logs"
	level  = zapcore.DebugLevel

	gossipClusters = []string{
		"10.181.22.31:8001",
		"10.181.22.31:8002",
		"10.181.22.31:8003",
	}
	seed       = []uint16{8001, 8002, 8003}
	namePrefix = "gossip-test-"

	membership = &RaftMembershipMessage{
		MemberInfos: map[uint64]*MemberInfo{
			1: {
				ClusterId:      1,
				ConfigChangeId: 1,
				Nodes: map[uint64]string{
					10001: "10.181.22.31:9001",
					10002: "10.181.22.31:9002",
					10003: "10.181.22.31:9003",
				},
				LeaderId:    10001,
				LeaderValid: true,
			},
			2: {
				ClusterId:      1,
				ConfigChangeId: 1,
				Nodes: map[uint64]string{
					10001: "10.181.22.31:9001",
					10002: "10.181.22.31:9002",
					10003: "10.181.22.31:9003",
				},
				LeaderId:    10002,
				LeaderValid: true,
			},
			3: {
				ClusterId:      1,
				ConfigChangeId: 1,
				Nodes: map[uint64]string{
					10001: "10.181.22.31:9001",
					10002: "10.181.22.31:9002",
					10004: "10.181.22.31:9004",
				},
				LeaderId:    10004,
				LeaderValid: true,
			},
			4: {
				ClusterId:      1,
				ConfigChangeId: 1,
				Nodes: map[uint64]string{
					10002: "10.181.22.31:9002",
					10003: "10.181.22.31:9003",
					10004: "10.181.22.31:9004",
				},
				LeaderId:    10003,
				LeaderValid: true,
			},
		},
	}

	clusterMessage = &RaftClusterMessage{
		Revision: 1,
		Targets: map[string]TargetClusterId{
			"raft-1": {
				GrpcAddr:   "10.181.22.31:6001",
				ClusterIds: []uint64{1, 2, 3},
			},
			"raft-2": {
				GrpcAddr:   "10.181.22.31:6002",
				ClusterIds: []uint64{1, 2, 3, 4},
			},
			"raft-3": {
				GrpcAddr:   "10.181.22.31:6003",
				ClusterIds: []uint64{1, 2, 4},
			},
			"raft-4": {
				GrpcAddr:   "10.181.22.31:6004",
				ClusterIds: []uint64{3, 4},
			},
		},
		Clusters: map[uint64][]string{
			1: {"raft-1", "raft-2", "raft-3"},
			2: {"raft-1", "raft-2", "raft-3"},
			3: {"raft-1", "raft-2", "raft-4"},
			4: {"raft-2", "raft-3", "raft-4"},
		},
	}
)

func TestGossip(t *testing.T) {
	clusters := []*GossipManager{}
	for i := 0; i < 3; i++ {
		cfg := GossipConfig{
			BindAddress: fmt.Sprintf("0.0.0.0:%d", seed[i]),
			BindPort:    seed[i],
			Seeds:       gossipClusters,
		}

		cfg.SetClusterCallback(WriteClusterToFile)

		name := namePrefix + fmt.Sprintf("%d", seed[i])
		moveToGrpcAddr := clusterMessage.Targets[fmt.Sprintf("raft-%d", i+1)].GrpcAddr
		gossipOpts := GossipOptions{
			Name:               name,
			MoveToGrpcAddr:     moveToGrpcAddr,
			LogDir:             logDir,
			LogLevel:           level,
			DisableCoordinates: true,
		}
		cluster, err := NewGossipManager(cfg, gossipOpts)
		if err != nil {
			t.Fatal(err)
		}

		cluster.SetNodeMeta(Meta{
			MoveToGrpcAddr: moveToGrpcAddr,
			RubikGrpcAddr:  moveToGrpcAddr,
		})

		clusters = append(clusters, cluster)
	}

	// 初始化原始的数据状态
	for _, cluster := range clusters {
		cluster.UpdateMembershipMessage(membership)
		cluster.UpdateClusterMessage(clusterMessage)
		time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	}

	// 查询初始化的数据是否与期望的相同
	cm := clusters[1].GetClusterMessage()
	if !reflect.DeepEqual(cm, clusterMessage) {
		t.Fatalf("init cluster message unexpected, %v, %v", cm, clusterMessage)
	}

	t.Logf("=================update=======================")

	cm.Revision += 1
	cm.Clusters = map[uint64][]string{
		1: {"raft-1", "raft-2", "raft-3"},
		2: {"raft-1", "raft-2", "raft-3", "raft-5"},
		3: {"raft-1", "raft-2", "raft-4"},
		4: {"raft-1", "raft-2", "raft-3", "raft-4", "raft-5"},
	}
	clusters[2].UpdateClusterMessage(cm)

	for retry := 0; retry < 3; retry++ {
		newCm := clusters[0].GetClusterMessage()
		if !reflect.DeepEqual(cm, newCm) && retry == 3 {
			t.Fatalf("update cluster message unexpected, %v, %v", cm, newCm)
		}
		time.Sleep(2 * time.Second)
	}

	time.Sleep(2 * time.Second)
	t.Logf("==================add cluster======================")

	cfg := GossipConfig{
		BindAddress: fmt.Sprintf("0.0.0.0:%d", 8004),
		BindPort:    8004,
		Seeds:       gossipClusters,
	}
	cfg.SetClusterCallback(WriteClusterToFile)
	moveToGrpcAddr := clusterMessage.Targets["raft-4"].GrpcAddr
	gossipOpts := GossipOptions{
		Name:               namePrefix + "8004",
		MoveToGrpcAddr:     moveToGrpcAddr,
		LogDir:             logDir,
		LogLevel:           level,
		DisableCoordinates: true,
	}
	cluster4, err := NewGossipManager(cfg, gossipOpts)
	if err != nil {
		t.Fatal(err)
	}
	cluster4.SetNodeMeta(Meta{
		MoveToGrpcAddr: moveToGrpcAddr,
		RubikGrpcAddr:  moveToGrpcAddr,
	})

	clusters = append(clusters, cluster4)
	// cluster4.UpdateMembershipMessage(membership)
	// cluster4.UpdateClusterMessage(clusterMessage)

	// 查询初始化的数据是否与期望的相同
	cm4 := cluster4.GetClusterMessage()
	if !reflect.DeepEqual(cm4, cm) {
		t.Fatalf("get init cluster4 message unexpected, %v, %v", cm4, clusterMessage)
	}

	time.Sleep(4 * time.Second)

	// 等待一段时间后, gossip协议应该会同步之前集群的数据
	cm4 = cluster4.GetClusterMessage()
	if !reflect.DeepEqual(cm4, cm) {
		t.Fatalf("get gossip sync cluster4 message unexpected, %v, %v", cm4, cm)
	}

	t.Logf("==================add cluster down======================")

	k := 1

	t.Log(clusters[k].GetAliveInstances())

	clusters[k].Close()

	time.Sleep(2 * time.Second)

	t.Log(clusters[k+1].GetAliveInstances())

	clusters = append(clusters[:k], clusters[k+1:]...)

	t.Logf("==================close cluster down======================")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	// free
	for _, cluster := range clusters {
		cluster.Close()
	}
}

func TestGossipCoordinate(t *testing.T) {
	clusters := []*GossipManager{}
	for i := 0; i < 3; i++ {
		cfg := GossipConfig{
			BindAddress: fmt.Sprintf("0.0.0.0:%d", seed[i]),
			BindPort:    seed[i],
			Seeds:       gossipClusters,
		}

		cfg.SetClusterCallback(WriteClusterToFile)

		name := namePrefix + fmt.Sprintf("%d", seed[i])
		moveToGrpcAddr := clusterMessage.Targets[fmt.Sprintf("raft-%d", i+1)].GrpcAddr
		gossipOpts := GossipOptions{
			Name:               name,
			MoveToGrpcAddr:     moveToGrpcAddr,
			LogDir:             logDir,
			LogLevel:           level,
			DisableCoordinates: false,
		}
		cluster, err := NewGossipManager(cfg, gossipOpts)
		if err != nil {
			t.Fatal(err)
		}

		cluster.SetNodeMeta(Meta{
			MoveToGrpcAddr: moveToGrpcAddr,
			RubikGrpcAddr:  moveToGrpcAddr,
		})

		clusters = append(clusters, cluster)
	}

	// 初始化原始的数据状态
	for _, cluster := range clusters {
		cluster.UpdateMembershipMessage(membership)
		cluster.UpdateClusterMessage(clusterMessage)
		time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	}

	// 查询初始化的数据是否与期望的相同
	cm := clusters[1].GetClusterMessage()
	if !reflect.DeepEqual(cm, clusterMessage) {
		t.Fatalf("init cluster message unexpected, %v, %v", cm, clusterMessage)
	}

	t.Logf("=================update=======================")

	// Make sure both nodes start out the origin so we can prove they did
	// an update later.
	c1, err := clusters[0].GetCoordinate()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	c2, err := clusters[1].GetCoordinate()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	const zeroThreshold = 20.0e-6
	if c1.DistanceTo(c2).Seconds() < zeroThreshold {
		t.Fatalf("coordinates didn't update after probes")
	}

	clusters[1].Close()

	clusters = append(clusters[:1], clusters[2:]...)

	t.Log(clusters[0].GetCachedCoordinate(namePrefix + fmt.Sprintf("%d", seed[1])))

	t.Logf("=================down=======================")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	// free
	for _, cluster := range clusters {
		cluster.Close()
	}
}

func TestGzipMsgpack(t *testing.T) {
	str := `{
		"revision": 2,
		"targets": {
			"nhid-11772876509704": {
				"grpcAddr": "10.181.22.31:24000",
				"clusterIds": [
					0,
					1,
					2,
					3,
					4
				]
			},
			"nhid-11772876509705": {
				"grpcAddr": "10.181.22.31:24001",
				"clusterIds": [
					0,
					1,
					5,
					6,
					7,
					2,
					3,
					4
				]
			},
			"nhid-11772876509706": {
				"grpcAddr": "10.181.22.31:24002",
				"clusterIds": [
					2,
					3,
					5,
					6
				]
			},
			"nhid-11772876509707": {
				"grpcAddr": "10.181.22.31:24003",
				"clusterIds": [
					0,
					2,
					4,
					5,
					7
				]
			},
			"nhid-11772876509708": {
				"grpcAddr": "10.181.22.31:24004",
				"clusterIds": [
					1,
					3,
					4,
					6,
					7
				]
			}
		},
		"clusters": {
			"0": [
				"nhid-11772876509704",
				"nhid-11772876509705",
				"nhid-11772876509707"
			],
			"1": [
				"nhid-11772876509704",
				"nhid-11772876509705",
				"nhid-11772876509708"
			],
			"2": [
				"nhid-11772876509704",
				"nhid-11772876509707",
				"nhid-11772876509706",
				"nhid-11772876509705"
			],
			"3": [
				"nhid-11772876509704",
				"nhid-11772876509708",
				"nhid-11772876509706",
				"nhid-11772876509705"
			],
			"4": [
				"nhid-11772876509704",
				"nhid-11772876509707",
				"nhid-11772876509708",
				"nhid-11772876509705"
			],
			"5": [
				"nhid-11772876509705",
				"nhid-11772876509707",
				"nhid-11772876509706"
			],
			"6": [
				"nhid-11772876509705",
				"nhid-11772876509708",
				"nhid-11772876509706"
			],
			"7": [
				"nhid-11772876509705",
				"nhid-11772876509707",
				"nhid-11772876509708"
			]
		},
		"initial_members": {
			"0": {
				"11772876509704": "nhid-11772876509704",
				"11772876509705": "nhid-11772876509705",
				"11772876509707": "nhid-11772876509707"
			},
			"1": {
				"11772876509704": "nhid-11772876509704",
				"11772876509705": "nhid-11772876509705",
				"11772876509708": "nhid-11772876509708"
			},
			"2": {
				"11772876509704": "nhid-11772876509704",
				"11772876509706": "nhid-11772876509706",
				"11772876509707": "nhid-11772876509707"
			},
			"3": {
				"11772876509704": "nhid-11772876509704",
				"11772876509706": "nhid-11772876509706",
				"11772876509708": "nhid-11772876509708"
			},
			"4": {
				"11772876509704": "nhid-11772876509704",
				"11772876509707": "nhid-11772876509707",
				"11772876509708": "nhid-11772876509708"
			},
			"5": {
				"11772876509705": "nhid-11772876509705",
				"11772876509706": "nhid-11772876509706",
				"11772876509707": "nhid-11772876509707"
			},
			"6": {
				"11772876509705": "nhid-11772876509705",
				"11772876509706": "nhid-11772876509706",
				"11772876509708": "nhid-11772876509708"
			},
			"7": {
				"11772876509705": "nhid-11772876509705",
				"11772876509707": "nhid-11772876509707",
				"11772876509708": "nhid-11772876509708"
			}
		},
		"join": {
			"0": {
				"11772876509704": false,
				"11772876509705": false,
				"11772876509707": false
			},
			"1": {
				"11772876509704": false,
				"11772876509705": false,
				"11772876509708": false
			},
			"2": {
				"11772876509704": false,
				"11772876509705": true,
				"11772876509706": false,
				"11772876509707": false
			},
			"3": {
				"11772876509704": false,
				"11772876509705": true,
				"11772876509706": false,
				"11772876509708": false
			},
			"4": {
				"11772876509704": false,
				"11772876509705": true,
				"11772876509707": false,
				"11772876509708": false
			},
			"5": {
				"11772876509705": false,
				"11772876509706": false,
				"11772876509707": false
			},
			"6": {
				"11772876509705": false,
				"11772876509706": false,
				"11772876509708": false
			},
			"7": {
				"11772876509705": false,
				"11772876509707": false,
				"11772876509708": false
			}
		}
	}`

	cluster := &RaftClusterMessage{}
	err := json.Unmarshal([]byte(str), cluster)
	if err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(1))

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err = encoder.Encode(cluster)
	if err != nil {
		t.Fatal(err)
	}

	b := buf.Bytes()

	len1 := len(b)

	gb, err := GZipEncode(b)
	if err != nil {
		t.Fatal(err)
	}

	len2 := len(gb)

	t.Logf("json length: %d, msgpack length: %d, gzip length: %d", len(str), len1, len2)

	bbuf, err := GZipDecode(gb)
	if err != nil {
		t.Fatal(err)
	}

	var handle2 codec.MsgpackHandle
	out := &RaftClusterMessage{}
	err = codec.NewDecoder(bytes.NewReader(bbuf[1:]), &handle2).Decode(out)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(out)
}

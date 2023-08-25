package gossip

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/memberlist"
)

// messageType are the types of gossip messages will send along
// memberlist.
type messageType uint8

const (
	messageClusterType messageType = iota
	messageMembershipType
	messagePushPullType
)

type TargetClusterId struct {
	GrpcAddr   string   `json:"grpcAddr"`
	ClusterIds []uint64 `json:"clusterIds"`
}

type RaftClusterMessage struct {
	Revision int64 `json:"revision"`

	// 机器ID对应的MoveTo的GRPC地址
	// key: 当raft以nodehostid=true的方式起的时候是机器ID，以固定地址方式起是raftAddr
	Targets map[string]TargetClusterId `json:"targets"`

	// 每个raft cluster对应的机器ID|raftAddr
	Clusters map[uint64][]string `json:"clusters"`

	// 每个raft cluster的initial members
	// key: clusterId, key: nodeId, val: raftAddr或nodeHostID
	InitialMembers map[uint64]map[uint64]string `json:"initial_members"`
	Join           map[uint64]map[uint64]bool   `json:"join"`
}

func (rm *RaftClusterMessage) String() string {
	b, _ := json.Marshal(rm)
	return string(b)
}

type MemberInfo struct {
	ClusterId      uint64
	ConfigChangeId uint64
	Nodes          map[uint64]string
	Observers      map[uint64]string
	LeaderId       uint64
	LeaderValid    bool
}

func (mi *MemberInfo) String() string {
	b, _ := json.Marshal(mi)
	return string(b)
}

type RaftMembershipMessage struct {
	// key: clusterId
	MemberInfos map[uint64]*MemberInfo
}

func (rm *RaftMembershipMessage) String() string {
	b, _ := json.Marshal(rm)
	return string(b)
}

type PushPullMessage struct {
	Cluster    *RaftClusterMessage
	Membership *RaftMembershipMessage
}

func (pp *PushPullMessage) String() string {
	b, _ := json.Marshal(pp)
	return string(b)
}

func decodeMessage(buf []byte, out interface{}) error {
	bbuf, err := GZipDecode(buf)
	if err != nil {
		return err
	}

	var handle codec.MsgpackHandle
	return codec.NewDecoder(bytes.NewReader(bbuf), &handle).Decode(out)
}

func encodeMessage(t messageType, msg interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err := encoder.Encode(msg)
	if err != nil {
		return nil, err
	}

	gbuf, err := GZipEncode(buf.Bytes())
	if err != nil {
		return nil, err
	}

	return append([]byte{uint8(t)}, gbuf...), nil
}

type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func newBroadcast(msg []byte) *broadcast {
	return &broadcast{
		msg:    msg,
		notify: make(chan struct{}),
	}
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

func GZipEncode(content []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(content); err != nil {
		return nil, err
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func GZipDecode(buf []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

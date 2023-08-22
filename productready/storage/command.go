package storage

import (
	"bytes"
	"context"
	"errors"
	"strconv"

	"github.com/xkeyideal/mraft/productready/storage/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/ugorji/go/codec"
)

var (
	revisionKey = []byte("__RAFT_KEY_REVISION__")
)

type CommandType byte

const (
	DELETE CommandType = 0
	PUT    CommandType = 1
	GET    CommandType = 2
)

type WriteOptions struct {
	// 存储key时，此key的revision
	Revision uint64
}

func mergeWriteOptions(opts ...*WriteOptions) *WriteOptions {
	wo := &WriteOptions{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}

		wo.Revision = opt.Revision
	}

	return wo
}

type RaftCommand interface {
	GetType() CommandType
	RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, clusterId uint64, session *client.Session) error
	LocalInvoke(s *store.Store, opts ...*WriteOptions) error
	GetResp() []byte
}

func Decode(buf []byte, e interface{}) error {
	handle := codec.MsgpackHandle{}
	return codec.NewDecoder(bytes.NewReader(buf), &handle).Decode(e)
}

func DecodeCmd(data []byte) (RaftCommand, error) {
	var cmd RaftCommand
	switch CommandType(data[0]) {
	case DELETE:
		cmd = &DelCommand{}
	case PUT:
		cmd = &PutCommand{}
	case GET:
		cmd = &GetCommand{}
	default:
		return nil, errors.New("can not find command type:" + strconv.Itoa(int(data[0])))
	}

	handle := codec.MsgpackHandle{}
	return cmd, codec.NewDecoder(bytes.NewReader(data[1:]), &handle).Decode(cmd)
}

func EncodeCmd(cmd RaftCommand) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(cmd.GetType()))

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err := encoder.Encode(cmd)
	return buf.Bytes(), err
}

func syncWrite(ctx context.Context, nh *dragonboat.NodeHost, session *client.Session, cmd RaftCommand) ([]byte, error) {
	b, err := EncodeCmd(cmd)
	if err != nil {
		return nil, err
	}

	result, err := nh.SyncPropose(ctx, session, b)
	if err != nil {
		return nil, err
	}

	return result.Data, nil
}

func syncRead(ctx context.Context, nh *dragonboat.NodeHost, clusterId uint64, cmd RaftCommand) ([]byte, error) {
	b, err := EncodeCmd(cmd)
	if err != nil {
		return nil, err
	}

	result, err := nh.SyncRead(ctx, clusterId, b)
	if err != nil {
		return nil, err
	}

	return result.([]byte), nil
}

func buildRevisionKey(key []byte) []byte {
	return append(revisionKey, key...)
}

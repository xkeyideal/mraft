package storage

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type CommandType byte

const (
	DELETE       CommandType = 0
	PUT          CommandType = 1
	SEARCH       CommandType = 2
	GET          CommandType = 3
	MUTLIDELETE  CommandType = 4
	DELETEPREFIX CommandType = 5
)

type RaftCommand interface {
	GetType() CommandType
	RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, clusterId uint64, session *client.Session) error
	LocalInvoke(s *Store) error
	GetResp() []byte
}

func DecodeCmd(data []byte) RaftCommand {
	var buf = bytes.NewBuffer(data[1:])
	var decoder = gob.NewDecoder(buf)

	switch CommandType(data[0]) {
	case DELETE:
		cmd := &DelCommand{}
		if err := decoder.Decode(cmd); err != nil {
			panic(err)
		}
		return cmd
	case PUT:
		cmd := &PutCommand{}
		if err := decoder.Decode(cmd); err != nil {
			panic(err)
		}
		return cmd
	case GET:
		cmd := &GetCommand{}
		if err := decoder.Decode(cmd); err != nil {
			panic(err)
		}
		return cmd
	}

	return nil
}

func EncodeCmd(cmd RaftCommand) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(cmd.GetType()))
	var encoder = gob.NewEncoder(&buf)
	if err := encoder.Encode(cmd); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func syncWrite(ctx context.Context, nh *dragonboat.NodeHost, session *client.Session, cmd RaftCommand) ([]byte, error) {
	result, err := nh.SyncPropose(ctx, session, EncodeCmd(cmd))
	if err != nil {
		return nil, err
	}
	return result.Data, err
}

func syncRead(ctx context.Context, nh *dragonboat.NodeHost, clusterId uint64, cmd RaftCommand) ([]byte, error) {
	result, err := nh.SyncRead(ctx, clusterId, EncodeCmd(cmd))
	if err != nil {
		return nil, err
	}
	return result.([]byte), err
}

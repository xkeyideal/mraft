package storage

import (
	"context"
	"errors"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/tecbot/gorocksdb"
)

type DelCommand struct {
	CfName string
	Key    []byte
}

func NewDelCommand(cfName string, key []byte) *DelCommand {
	return &DelCommand{CfName: cfName, Key: key}
}

func (c *DelCommand) GetType() CommandType {
	return DELETE
}

func (c *DelCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, _ uint64, session *client.Session) error {
	_, err := syncWrite(ctx, nh, session, c)
	return err
}

func (c *DelCommand) LocalInvoke(s *Store) error {
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	cfHandle, err := s.GetCfHandle(c.CfName)
	if err != nil {
		return errors.New("can not find cfName:" + c.CfName)
	}

	batch.DeleteCF(cfHandle, c.Key)

	return s.Write(batch)
}

func (c *DelCommand) GetResp() []byte {
	return nil
}

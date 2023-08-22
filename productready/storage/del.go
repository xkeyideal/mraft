package storage

import (
	"context"

	"github.com/xkeyideal/mraft/productready/storage/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
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

func (c *DelCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	batch := s.Batch()
	defer batch.Close()

	cf := s.GetColumnFamily(c.CfName)

	batch.Delete(s.BuildColumnFamilyKey(cf, c.Key), s.GetWo())

	// 删除revision
	revisionKey := buildRevisionKey(c.Key)
	batch.Delete(s.BuildColumnFamilyKey(cf, revisionKey), s.GetWo())

	return s.Write(batch)
}

func (c *DelCommand) GetResp() []byte {
	return nil
}

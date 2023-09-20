package storage

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/xkeyideal/mraft/productready/storage/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type PutCommand struct {
	Cf    string
	Key   []byte
	Value []byte
}

func (c *PutCommand) GetResp() []byte {
	return nil
}

func NewPutCommand(cf string, key, value []byte) *PutCommand {
	return &PutCommand{Cf: cf, Key: key, Value: value}
}

func (c *PutCommand) GetType() CommandType {
	return PUT
}

func (c *PutCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, _ uint64, session *client.Session) error {
	_, err := syncWrite(ctx, nh, session, c)
	return err
}

func (c *PutCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	batch := s.Batch()
	defer batch.Close()

	cf := s.GetColumnFamily(c.Cf)

	batch.Delete(s.BuildColumnFamilyKey(cf, c.Key), pebble.Sync)

	// 删除revision
	revisionKey := buildRevisionKey(c.Key)
	batch.Delete(s.BuildColumnFamilyKey(cf, revisionKey), pebble.Sync)

	return s.Write(batch)

	return s.Write(batch)
}

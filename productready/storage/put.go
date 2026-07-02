package storage

import (
	"context"
	"encoding/binary"

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
	wo := mergeWriteOptions(opts...)

	batch := s.Batch()
	defer batch.Close()

	cf, err := s.GetColumnFamily(c.Cf)
	if err != nil {
		return err
	}

	revisionValue := make([]byte, 8)
	binary.BigEndian.PutUint64(revisionValue, wo.Revision)

	// set revision
	batch.Set(s.BuildColumnFamilyKey(cf, buildRevisionKey(c.Key)), revisionValue, s.GetWo())

	batch.Set(s.BuildColumnFamilyKey(cf, c.Key), c.Value, s.GetWo())

	return s.Write(batch)
}

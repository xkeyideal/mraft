package storage

import (
	"context"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/tecbot/gorocksdb"
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

func (c *PutCommand) LocalInvoke(s *Store) error {
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	cfHandle, err := s.GetCfHandle(c.Cf)
	if err != nil {
		return err
	}

	batch.PutCF(cfHandle, c.Key, c.Value)

	return s.Write(batch)
}

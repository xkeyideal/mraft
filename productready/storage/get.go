package storage

import (
	"context"
	"encoding/binary"

	"github.com/xkeyideal/mraft/productready/storage/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
)

type GetCommand struct {
	Cf   string
	Key  []byte
	resp []byte
}

func (c *GetCommand) GetResp() []byte {
	return c.resp
}

func NewGetCommand(cf string, key []byte) *GetCommand {
	return &GetCommand{Cf: cf, Key: key}
}

func (c *GetCommand) GetType() CommandType {
	return GET
}

func (c *GetCommand) RaftInvoke(ctx context.Context, nh *dragonboat.NodeHost, clusterId uint64, _ *client.Session) (err error) {
	c.resp, err = syncRead(ctx, nh, clusterId, c)
	return err
}

func (c *GetCommand) LocalInvoke(s *store.Store, opts ...*WriteOptions) error {
	cf := s.GetColumnFamily(c.Cf)

	// get revision
	v, err := s.GetBytes(s.BuildColumnFamilyKey(cf, buildRevisionKey(c.Key)))
	if err != nil {
		return err
	}

	if len(v) == 0 {
		v = make([]byte, 8)
		binary.BigEndian.PutUint64(v, 0)
	}

	// get value
	d, err := s.GetBytes(s.BuildColumnFamilyKey(cf, c.Key))
	if err != nil {
		return err
	}

	c.resp = append(v, d...)
	return nil
}

func (c *GetCommand) GetResult() []byte {
	return c.resp
}

package storage

import (
	"context"

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

func (c *GetCommand) LocalInvoke(s *Store) error {
	d, err := s.GetBytes(c.Cf, c.Key)
	c.resp = d
	return err
}

func (c *GetCommand) GetResult() []byte {
	return c.resp
}

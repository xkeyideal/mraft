package store

import (
	"fmt"

	thrifter "github.com/thrift-iterator/go"
)

const (
	CommandDelete = "delete"
	CommandUpsert = "upsert"
)

type Command struct {
	Cmd string `thrift:"Cmd,1"`
	Key string `thrift:"Key,2"`
	Val string `thrift:"Val,3"`
}

func NewCommand(cmd, key, val string) *Command {
	return &Command{
		Cmd: cmd,
		Key: key,
		Val: val,
	}
}

func (cmd *Command) Unmarshal(b []byte) error {
	return thrifter.Unmarshal(b, cmd)
}

func (cmd *Command) Marshal() ([]byte, error) {
	return thrifter.Marshal(cmd)
}

type RaftAttribute struct {
	AttrID    int64             `thrift:"AttrID,1" db:"AttrID" json:"AttrID"`
	AttrName  string            `thrift:"AttrName,2" db:"AttrName" json:"AttrName"`
	Ages      []int32           `thrift:"Ages,3" db:"Ages" json:"Ages"`
	Locations map[string]string `thrift:"Locations,4" db:"Locations" json:"Locations"`
	Timestamp int64             `thrift:"Timestamp,5" db:"Timestamp" json:"Timestamp"`
}

func (attr *RaftAttribute) Marshal() ([]byte, error) {
	return thrifter.Marshal(attr)
}

func (attr *RaftAttribute) GenerateCommand(cmd string) (*Command, error) {
	b, err := attr.Marshal()
	if err != nil {
		return nil, err
	}

	return NewCommand(cmd, fmt.Sprintf("%d_%s", attr.AttrID, attr.AttrName), string(b)), nil
}

func (attr *RaftAttribute) Unmarshal(b []byte) error {
	return thrifter.Unmarshal(b, attr)
}

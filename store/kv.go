package store

import (
	"encoding/binary"
	"fmt"
	"io"

	thrifter "github.com/thrift-iterator/go"
)

const (
	CommandDelete = "delete"
	CommandUpsert = "upsert"
)

type Command struct {
	Cmd     string `thrift:"Cmd,1"`
	HashKey uint64 `thrift:"HashKey,2"`
	Key     string `thrift:"Key,3"`
	Val     string `thrift:"Val,4"`
}

func NewCommand(cmd, key, val string, hashKey uint64) *Command {
	return &Command{
		Cmd:     cmd,
		HashKey: hashKey,
		Key:     key,
		Val:     val,
	}
}

func (cmd *Command) Unmarshal(b []byte) error {
	return thrifter.Unmarshal(b, cmd)
}

func (cmd *Command) Marshal() ([]byte, error) {
	return thrifter.Marshal(cmd)
}

type RaftAttribute struct {
	AttrID    uint64            `thrift:"AttrID,1" db:"AttrID" json:"AttrID"`
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

	return NewCommand(cmd, fmt.Sprintf("%d_%s", attr.AttrID, attr.AttrName), string(b), attr.AttrID), nil
}

func (attr *RaftAttribute) Unmarshal(b []byte) error {
	return thrifter.Unmarshal(b, attr)
}

func (attr *RaftAttribute) WriteTo(w io.Writer) (int64, error) {
	dataSize := make([]byte, 8)

	b, err := attr.Marshal()
	if err != nil {
		return 0, err
	}

	l := len(b)

	binary.LittleEndian.PutUint64(dataSize, uint64(l))
	if _, err := w.Write(dataSize); err != nil {
		return 0, err
	}

	if _, err := w.Write(b); err != nil {
		return 0, err
	}

	return int64(8 + l), nil
}

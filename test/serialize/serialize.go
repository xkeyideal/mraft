package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

func encode(key, val []byte, w io.Writer) error {
	dataSize := make([]byte, 8)
	keySize := make([]byte, 8)
	valSize := make([]byte, 8)

	kl := len(key)
	vl := len(val)

	binary.LittleEndian.PutUint64(dataSize, uint64(kl+vl+8+8))
	if _, err := w.Write(dataSize); err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(keySize, uint64(kl))
	if _, err := w.Write(keySize); err != nil {
		return err
	}

	if _, err := w.Write(key); err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(valSize, uint64(vl))
	if _, err := w.Write(valSize); err != nil {
		return err
	}

	if _, err := w.Write(val); err != nil {
		return err
	}

	return nil
}

func decode(r io.Reader) ([]byte, []byte, error) {
	sz := make([]byte, 8)
	if _, err := io.ReadFull(r, sz); err != nil {
		return nil, nil, err
	}
	dataSize := binary.LittleEndian.Uint64(sz)
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, nil, err
	}

	kl := binary.LittleEndian.Uint64(data[:8])
	key := data[8 : kl+8]
	vl := binary.LittleEndian.Uint64(data[kl+8 : kl+16])
	val := data[kl+16:]
	if uint64(len(val)) != vl {
		return nil, nil, errors.New("size isn't equal")
	}

	return key, val, nil
}

func main() {
	key := []byte("multi-raft-key")
	val := []byte("multi-raft-value")

	buf := &bytes.Buffer{}
	err := encode(key, val, buf)
	if err != nil {
		panic(err)
	}

	key1, val1, err := decode(buf)
	fmt.Println(string(key1), string(val1), err)
}

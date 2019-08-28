package serialize

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
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

func TestBinarySerialize(t *testing.T) {
	key := []byte("multi-raft-key")
	val := []byte("multi-raft-value")

	buf := &bytes.Buffer{}
	err := encode(key, val, buf)
	if err != nil {
		t.Fatalf("binary marshal fatal, %+v", err)
		return
	}

	key1, val1, err := decode(buf)
	if err != nil {
		t.Fatalf("binary unmarshal fatal, %+v", err)
		return
	}

	if !bytes.Equal(key1, key) {
		t.Fatalf("binary unmarshal expected %v, got %v", key, key1)
		return
	}

	if !bytes.Equal(val1, val) {
		t.Fatalf("binary unmarshal expected %v, got %v", val, val1)
		return
	}
}

func BenchmarkBinaryEncode(b *testing.B) {
	key := []byte("multi-raft-key")
	val := []byte("multi-raft-value")

	buf := &bytes.Buffer{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encode(key, val, buf)
	}
}

func BenchmarkBinaryDecode(b *testing.B) {
	key := []byte("multi-raft-key")
	val := []byte("multi-raft-value")

	buf := &bytes.Buffer{}
	err := encode(key, val, buf)
	if err != nil {
		b.Fatalf("binary marshal fatal, %+v", err)
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decode(buf)
	}
}

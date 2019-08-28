package serialize

import (
	"mraft/store"
	"reflect"
	"testing"
)

func TestMarshalByThrift(t *testing.T) {
	cmd := store.NewCommand("command", "key", "value", 10000)
	b, err := cmd.Marshal()
	if err != nil {
		t.Fatalf("thrift marshal fatal, %+v", err)
		return
	}

	cmd2 := &store.Command{}
	err = cmd2.Unmarshal(b)
	if err != nil {
		t.Fatalf("thrift unmarshal fatal, %+v", err)
		return
	}

	if !reflect.DeepEqual(cmd, cmd2) {
		t.Fatalf("thrift unmarshal expected %v, got %v", cmd, cmd2)
	}

	t.Logf("%+v, %+v", cmd, cmd2)
}

func BenchmarkMarshalByThrift(b *testing.B) {
	cmd := store.NewCommand("command", "key", "value", 10000)

	for i := 0; i < b.N; i++ {
		cmd.Marshal()
	}
}

func BenchmarkUnmarshalByThrift(b *testing.B) {
	cmd := store.NewCommand("command", "key", "value", 10000)
	bs, err := cmd.Marshal()
	if err != nil {
		b.Fatalf("thrift marshal fatal, %+v", err)
		return
	}

	cmd2 := &store.Command{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd2.Unmarshal(bs)
	}
}

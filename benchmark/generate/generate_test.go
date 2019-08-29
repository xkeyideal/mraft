package generate

import "testing"

func TestGenerateAttr(t *testing.T) {
	attr := GenerateData()
	t.Logf("%+v", attr)
}

func BenchmarkGenerateAttr(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateData()
	}
}

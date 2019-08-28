go test -bench=. -benchmem   

<br>

```
goos: darwin
goarch: amd64
pkg: mraft/benchmark/binary-serialize
BenchmarkBinaryEncode-4         10000000               214 ns/op             149 B/op          3 allocs/op
BenchmarkBinaryDecode-4         50000000                29.9 ns/op             8 B/op          1 allocs/op
PASS
ok      mraft/benchmark/binary-serialize        4.059s
```
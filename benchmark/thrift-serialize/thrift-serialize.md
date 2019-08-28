go test -bench=. -benchmem   

<br>

```
goos: darwin
goarch: amd64
pkg: mraft/benchmark/thrift-serialize
BenchmarkMarshalByThrift-4       3000000               413 ns/op             208 B/op          6 allocs/op
BenchmarkUnmarshalByThrift-4     3000000               418 ns/op             152 B/op          5 allocs/op
PASS
ok      mraft/benchmark/thrift-serialize        3.343s
```
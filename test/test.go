package main

import (
	"fmt"
)

func main() {
	b := []byte("1234")
	bb := make([]byte, len(b))

	copy(bb, b)
	fmt.Println(string(bb))

	b[2] = '5'

	fmt.Println(string(bb), string(b))
}

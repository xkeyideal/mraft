package main

import (
	"encoding/json"
	"fmt"
)

type kv struct {
	Key string `json:"key"`
	Val int    `json:"val"`
}

func main() {
	// s := metrics.NewExpDecaySample(2028, 0.015) // or metrics.NewUniformSample(1028)
	// h := metrics.NewHistogram(s)
	// metrics.Register("baz", h)

	// for i := 90; i < 105; i++ {
	// 	h.Update(int64(i))
	// }

	// fmt.Println(h.Min(), h.Max(), h.Mean(), h.Percentiles([]float64{0.9, 0.95, 0.99}))

	// for i := 0; i < 10; i++ {
	// 	rand.Seed(time.Now().UnixNano())
	// 	fmt.Println(rand.Int31n(1000000))
	// }

	// d := kv{"2", 2}
	// b, _ := json.Marshal(d)
	// fmt.Println(string(b))

	b := `{"key":"2","val":2}`
	d := kv{}
	json.Unmarshal([]byte(b), &d)
	fmt.Println(d)
}

package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	// s := metrics.NewExpDecaySample(2028, 0.015) // or metrics.NewUniformSample(1028)
	// h := metrics.NewHistogram(s)
	// metrics.Register("baz", h)

	// for i := 90; i < 105; i++ {
	// 	h.Update(int64(i))
	// }

	// fmt.Println(h.Min(), h.Max(), h.Mean(), h.Percentiles([]float64{0.9, 0.95, 0.99}))

	for i := 0; i < 10; i++ {
		rand.Seed(time.Now().UnixNano())
		fmt.Println(rand.Int31n(1000000))
	}

}

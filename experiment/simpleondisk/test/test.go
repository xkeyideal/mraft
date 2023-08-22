package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/xkeyideal/gokit/httpkit"
)

const (
	HttpConnTimeout      = 3 * time.Second
	HttpReadWriteTimeout = 5 * time.Second
	HttpRetry            = 0
	HttpRetryInterval    = 2 * time.Second
)

func doHttp(addr string, val int, wg *sync.WaitGroup) {

	defer wg.Done()

	client := httpkit.NewHttpClient(HttpReadWriteTimeout, HttpRetry, HttpRetryInterval, HttpConnTimeout, nil)

	client = client.SetParam("key", "simple_4").SetParam("hashKey", "123").SetParam("val", strconv.Itoa(val))

	resp, err := client.Post(fmt.Sprintf("http://%s/msimpleraft/key", addr))

	if err != nil {
		fmt.Println(addr, err)
		return
	}

	if resp.StatusCode != 200 {
		fmt.Println(resp.StatusCode)
	}
}

func doQuery(addr string) {
	client := httpkit.NewHttpClient(HttpReadWriteTimeout, HttpRetry, HttpRetryInterval, HttpConnTimeout, nil)

	client = client.SetParam("key", "simple_4").SetParam("hashKey", "123")

	resp, err := client.Get(fmt.Sprintf("http://%s/msimpleraft/key", addr))

	if err != nil {
		fmt.Println(addr, err)
		return
	}

	if resp.StatusCode != 200 {
		fmt.Println(resp.StatusCode)
		return
	}

	fmt.Println(string(resp.Body))
}

func main() {
	addrs := []string{"10.101.44.4:10100", "10.101.44.4:10101", "10.101.44.4:10102"}

	n := 10

	wg := &sync.WaitGroup{}
	wg.Add(n)

	for i := 0; i < n; i++ {
		go doHttp(addrs[i%3], i, wg)
	}

	wg.Wait()

	doQuery(addrs[0])
}

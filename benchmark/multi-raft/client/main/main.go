package main

import (
	"fmt"
	"mraft/benchmark/generate"
	"mraft/benchmark/multi-raft/client"
	"mraft/store"
	"sync"
	"time"
)

type TestClient struct {
	client *client.SimpleClient
	send   chan *store.RaftAttribute
	recv   chan struct{}

	exitchan chan struct{}
	wg       sync.WaitGroup
}

func (tc *TestClient) Gen(n int) {
	for i := 0; i < n; i++ {
		attr := generate.GenerateData()
		tc.send <- attr
	}
}

func (tc *TestClient) Send() {
	for {
		select {
		case <-tc.exitchan:
			return
		case attr := <-tc.send:
			tc.client.SendMessage(attr)
		}
	}
}

func (tc *TestClient) Recv() {
	for {
		select {
		case <-tc.exitchan:
			return
		case <-tc.recv:
			tc.wg.Done()
		}
	}
}

func (tc *TestClient) Stop() {
	close(tc.exitchan)
	tc.client.Stop()
}

func makeClient(connections, taskNum int, wg *sync.WaitGroup) {

	cwg := &sync.WaitGroup{}
	cwg.Add(connections)

	for i := 0; i < connections; i++ {
		go func(cwg *sync.WaitGroup) {
			tc := &TestClient{
				send:     make(chan *store.RaftAttribute, 1000),
				recv:     make(chan struct{}, 1000),
				exitchan: make(chan struct{}),
				wg:       sync.WaitGroup{},
			}

			client, err := client.NewSimpleClient("10.101.44.4:25700", tc.recv)
			if err != nil {
				panic(err)
			}

			tc.client = client

			tc.wg.Add(taskNum)

			go tc.Gen(taskNum)
			go tc.Send()
			go tc.Recv()

			tc.wg.Wait()

			cwg.Done()
		}(cwg)
	}

	cwg.Wait()

	wg.Done()
}

func main() {
	st := time.Now()

	g, c, n := 10, 4, 1000

	wg := &sync.WaitGroup{}

	wg.Add(g)

	for i := 0; i < g; i++ {
		go makeClient(c, n, wg)
	}

	wg.Wait()

	ed := time.Now()
	op := float64(ed.UnixNano()-st.UnixNano()) / float64(n*1000)

	fmt.Printf("线程数：%d, 每个线程连接数：%d, 请求次数：%d, 平均耗时：%.1f us/op\n", g, c, n, op)
}

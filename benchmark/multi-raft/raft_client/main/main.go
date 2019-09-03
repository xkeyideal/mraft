package main

import (
	"fmt"
	"math/rand"
	"mraft/benchmark/generate"
	"mraft/benchmark/multi-raft/raft_client"
	"mraft/store"
	"sync"
	"time"
)

type TestClient struct {
	client *raft_client.RaftSimpleClient
	send   chan *store.RaftAttribute
	query  chan *store.ReadArgument

	exitchan chan struct{}
	wg       sync.WaitGroup
}

func (tc *TestClient) Gen(n int) {
	for i := 0; i < n; i++ {
		attr := generate.GenerateData()
		tc.send <- attr
	}
}

func (tc *TestClient) SendWriteCommand() {
	for {
		select {
		case <-tc.exitchan:
			return
		case attr := <-tc.send:
			fmt.Println("before PublishCommand")
			_, err := tc.client.PublishCommand(attr)
			fmt.Println("after PublishCommand", err)
			if err != nil {
				fmt.Println("SendWriteCommand: ", err.Error())
			}

			n := rand.Intn(10)
			fmt.Println("random_n:", n)
			if n < 10 { // 30%入查询
				tc.query <- &store.ReadArgument{
					Key:     fmt.Sprintf("%d_%s", attr.AttrID, attr.AttrName),
					HashKey: attr.AttrID,
					Sync:    true,
				}
			} else {
				tc.wg.Done()
			}
		}
	}
}

func (tc *TestClient) SendQueryCommand() {
	for {
		select {
		case <-tc.exitchan:
			return
		case arg := <-tc.query:
			attr, err := tc.client.PublishCommand(arg)
			fmt.Println(attr, err)
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
				send:     make(chan *store.RaftAttribute, 100),
				query:    make(chan *store.ReadArgument, 30),
				exitchan: make(chan struct{}),
				wg:       sync.WaitGroup{},
			}

			client, err := raft_client.NewRaftSimpleClient("10.101.44.4:25700")
			if err != nil {
				panic(err)
			}

			tc.client = client

			tc.wg.Add(taskNum)

			go tc.Gen(taskNum)
			go tc.SendWriteCommand()
			go tc.SendQueryCommand()

			tc.wg.Wait()

			cwg.Done()
		}(cwg)
	}

	cwg.Wait()

	wg.Done()
}

func main() {
	st := time.Now()

	g, c, n := 1, 1, 1

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

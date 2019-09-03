package main

import (
	"fmt"
	"mraft/benchmark/generate"
	"mraft/benchmark/multi-raft/raft_client"
	"mraft/store"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

var histogram metrics.Histogram
var counter metrics.Counter

var servers = []string{"10.101.44.4:25700", "10.101.44.4:25800", "10.101.44.4:25900"}

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
			st := time.Now().UnixNano()
			_, err := tc.client.PublishCommand(attr)
			if err != nil {
				counter.Inc(1)
			}

			x := time.Now().UnixNano() - st
			histogram.Update(x)

			// n := rand.Intn(10)
			// fmt.Println("random_n:", n)
			// if n < 10 { // 30%入查询
			// 	tc.query <- &store.ReadArgument{
			// 		Key:     fmt.Sprintf("%d_%s", attr.AttrID, attr.AttrName),
			// 		HashKey: attr.AttrID,
			// 		Sync:    true,
			// 	}
			// } else {
			tc.wg.Done()
			// }
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

func makeClient(index, connections, taskNum int, wg *sync.WaitGroup) {

	cwg := &sync.WaitGroup{}
	cwg.Add(connections)

	for i := 0; i < connections; i++ {
		go func(index int, cwg *sync.WaitGroup) {
			tc := &TestClient{
				send:     make(chan *store.RaftAttribute, 100),
				query:    make(chan *store.ReadArgument, 30),
				exitchan: make(chan struct{}),
				wg:       sync.WaitGroup{},
			}

			client, err := raft_client.NewRaftSimpleClient(servers[index%3])
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
		}(index, cwg)
	}

	cwg.Wait()

	wg.Done()
}

func main() {
	g, c, n := 6, 1, 1000

	s := metrics.NewExpDecaySample(10240, 0.015) // or metrics.NewUniformSample(1028)
	histogram = metrics.NewHistogram(s)

	counter = metrics.NewCounter()

	wg := &sync.WaitGroup{}

	wg.Add(g)

	for i := 0; i < g; i++ {
		go makeClient(i, c, n, wg)
	}

	wg.Wait()

	fmt.Printf("总次数: %d, 错误数: %d, 线程数: %d, 每个线程连接数: %d, 请求次数: %d\n", histogram.Count(), counter.Count(), g, c, n)
	fmt.Printf("最小值: %dus, 最大值: %dus, 中间值: %.1fus\n", histogram.Min()/1e3, histogram.Max()/1e3, histogram.Mean()/1e3)
	fmt.Printf("75百分位: %.1fus, 90百分位: %.1fus, 95百分位: %.1fus, 99百分位: %.1fus\n", histogram.Percentile(0.75)/1e3, histogram.Percentile(0.9)/1e3, histogram.Percentile(0.95)/1e3, histogram.Percentile(0.99)/1e3)
}

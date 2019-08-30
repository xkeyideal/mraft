package main

import (
	"fmt"
	"mraft/benchmark/generate"
	"mraft/benchmark/multi-raft/client"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	client, err := client.NewSimpleClient("10.101.44.4:25700")
	if err != nil {
		panic(err)
	}

	attr := generate.GenerateData()
	fmt.Printf("%+v\n", attr)
	client.SendMessage(attr)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-signals

	client.Stop()
}

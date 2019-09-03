package main

import (
	"mraft/benchmark/multi-raft/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	server, err := server.NewSimpleServer("10.101.44.4:25701")
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-signals

	server.Stop()
}

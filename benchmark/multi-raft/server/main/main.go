package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/xkeyideal/mraft/benchmark/multi-raft/server"
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

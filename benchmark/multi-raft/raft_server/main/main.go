package main

import (
	"fmt"
	"mraft/benchmark/multi-raft/raft_server"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	nodeID, err := strconv.ParseUint(os.Args[1], 10, 64)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	port := os.Args[2]

	// nodeID: 10000, 10001, 10002
	// port: 25700, 25800, 25900
	server, err := raft_server.NewRaftSimpleServer(fmt.Sprintf("10.101.44.4:%s", port), nodeID)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-signals

	server.Stop()
}

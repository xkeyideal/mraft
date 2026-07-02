package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/xkeyideal/mraft/experiment/simpleondisk/httpengine"
)

func usage() {
	fmt.Println("Usage: go run main.go -node <nodeID> -port <httpPort> [options]")
	flag.PrintDefaults()
}

func main() {
	var (
		node         = flag.Uint64("node", 0, "node ID, e.g. 10000")
		port         = flag.Int("port", 0, "HTTP port")
		dir          = flag.String("dir", "./raftdata-simpleondisk", "raft data directory")
		ip           = flag.String("ip", "127.0.0.1", "raft listen IP")
		baseRaftPort = flag.Uint64("base-raft-port", 34000, "raft port for node 10000, other nodes are base + (nodeID - 10000)")
	)
	flag.Usage = usage
	flag.Parse()

	if *node == 0 || *port == 0 {
		usage()
		os.Exit(1)
	}

	engine := httpengine.NewEngine(*node, *port, *dir, *ip, *baseRaftPort)

	go engine.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-signals

	engine.Stop()
}

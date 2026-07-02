package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/xkeyideal/mraft/experiment/ondisk/engine"
)

func usage() {
	fmt.Println("Usage: go run app.go -node <nodeID> -port <httpPort> [options]")
	flag.PrintDefaults()
}

func main() {
	var (
		node         = flag.Uint64("node", 0, "node ID, e.g. 10000")
		port         = flag.Int("port", 0, "HTTP port")
		dir          = flag.String("dir", "./raftdata-ondisk", "raft data directory")
		ip           = flag.String("ip", "127.0.0.1", "raft listen IP")
		baseRaftPort = flag.Uint64("base-raft-port", 11000, "raft port for node 10000, other nodes are base + (nodeID - 10000)")
		peerIDs      = flag.String("peers", "10000,10001,10002,10003,10004,10005", "comma separated node IDs in the cluster")
	)
	flag.Usage = usage
	flag.Parse()

	if *node == 0 || *port == 0 {
		usage()
		os.Exit(1)
	}

	raftPort := *baseRaftPort + (*node - 10000)
	raftAddr := fmt.Sprintf("%s:%d", *ip, raftPort)

	initialPeers := make(map[uint64]string)
	for _, s := range strings.Split(*peerIDs, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		id, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			fmt.Printf("invalid peer id %q: %v\n", s, err)
			os.Exit(1)
		}
		// 10003/10004/10005 在示例中是后加入节点，不应出现在 InitialMembers 中
		if id == 10003 || id == 10004 || id == 10005 {
			continue
		}
		p := *baseRaftPort + (id - 10000)
		initialPeers[id] = fmt.Sprintf("%s:%d", *ip, p)
	}

	engine := engine.NewEngine(*node, *port, *dir, raftAddr, initialPeers)

	go engine.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-signals

	engine.Stop()
}

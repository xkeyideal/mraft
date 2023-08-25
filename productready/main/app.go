package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/xkeyideal/mraft/productready"
	"github.com/xkeyideal/mraft/productready/config"
)

var DevelopDefaultDynamicConfig = &config.DynamicConfig{
	RaftDir: "/Users/xkey/test/raftdata",
	LogDir:  "/tmp/",
	IP:      "127.0.0.1",
}

func main() {
	if len(os.Args) <= 2 {
		log.Fatal("input arg $1 httpPort, $2 raftPort")
	}

	config := DevelopDefaultDynamicConfig

	httpPort, err := strconv.ParseUint(os.Args[1], 10, 64)
	if err != nil {
		log.Fatal("[ERROR]", err)
	}

	config.HttpPort = uint16(httpPort)

	raftPort, err := strconv.ParseUint(os.Args[1], 10, 64)
	if err != nil {
		log.Fatal("[ERROR]", err)
	}

	config.RaftPort = uint16(raftPort)

	eg := productready.NewEngine(config)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	eg.Stop()
}

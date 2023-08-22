package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/xkeyideal/mraft/experiment/simpleondisk/httpengine"
)

// CGO_CFLAGS="-I/usr/local/include/rocksdb" CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4" go run app.go 10000 9800
func main() {
	if len(os.Args) <= 2 {
		fmt.Println("input arg $1 nodeID, arg $2 port")
		os.Exit(1)
	}

	nodeID, err := strconv.ParseUint(os.Args[1], 10, 64)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	port := os.Args[2]

	engine := httpengine.NewEngine(nodeID, port)

	go engine.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-signals

	engine.Stop()
}

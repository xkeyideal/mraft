package cmd

import (
	"log"
	"os"
	"strconv"
)

func SavePidToFile() {
	pid := os.Getpid()
	wf, err := os.OpenFile("/var/run/"+pidFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal(err)
	}
	_, _ = wf.WriteString(strconv.Itoa(pid))
	_ = wf.Sync()
	_ = wf.Close()
}

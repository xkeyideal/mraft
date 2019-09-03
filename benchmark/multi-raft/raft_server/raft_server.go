package raft_server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"mraft/ondisk"
	"mraft/store"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"
)

const defaultBufferSize = 5 * 1024

var (
	raftDataDir   = "/Volumes/ST1000/mraft-server-ondisk"
	raftNodePeers = map[uint64]string{
		10000: "10.101.44.4:54000",
		10001: "10.101.44.4:54100",
		10002: "10.101.44.4:54200",
	}
	raftClusterIDs = []uint64{254000, 254100, 254200}
)

type clientConn struct {
	writeLock sync.RWMutex
	net.Conn

	Reader *bufio.Reader
	Writer *bufio.Writer
}

func newClientConn(conn net.Conn) *clientConn {
	return &clientConn{
		Conn:   conn,
		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),
	}
}

type RaftSimpleServer struct {
	writelock *sync.Mutex

	nh *ondisk.OnDiskRaft

	tcpListener net.Listener
}

func NewRaftSimpleServer(address string, nodeID uint64) (*RaftSimpleServer, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	ss := &RaftSimpleServer{
		writelock:   &sync.Mutex{},
		nh:          ondisk.NewOnDiskRaft(raftNodePeers, raftClusterIDs),
		tcpListener: l,
	}

	ss.nh.Start(raftDataDir, nodeID, "", false)

	go ss.RaftTCPServer()

	return ss, nil
}

func (ss *RaftSimpleServer) RaftTCPServer() error {
	for {
		conn, err := ss.tcpListener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}

		go ss.handle(conn)
	}

	return nil
}

func (ss *RaftSimpleServer) handle(conn net.Conn) error {

	var err error

	client := newClientConn(conn)

	sz := make([]byte, 8)

	for {
		client.SetReadDeadline(time.Now().Add(3 * time.Second))

		_, err = client.Reader.Read(sz)
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read datasize - %s", err)
			}
			break
		}

		dataSize := binary.LittleEndian.Uint64(sz)
		body := make([]byte, dataSize)

		_, err = io.ReadFull(client.Reader, body)
		if err != nil {
			err = fmt.Errorf("failed to read databody - %s", err)
			break
		}

		cmdSize := binary.LittleEndian.Uint32(body[:4])

		command := string(body[4 : 4+cmdSize])

		data := body[4+cmdSize:]

		attr, err := ss.execCommand(command, data)

		fmt.Println(attr, err)

		ss.sendResponse(client, command, attr, err)
	}

	return err
}

func (ss *RaftSimpleServer) execCommand(command string, data []byte) (*store.RaftAttribute, error) {
	fmt.Println("execCommand: ", command)
	switch command {
	case store.CommandRead:
		arg := &store.ReadArgument{}
		err := arg.Unmarshal(data)

		//fmt.Printf("arg: %+v\n", arg)
		if err != nil {
			return nil, err
		}

		if arg.Sync {
			return ss.nh.SyncRead(arg.Key, arg.HashKey)
		}
		return ss.nh.ReadLocal(arg.Key, arg.HashKey)
	case store.CommandUpsert:
		attr := &store.RaftAttribute{}
		err := attr.Unmarshal(data)
		//fmt.Printf("attr: %+v\n", attr)
		if err != nil {
			return nil, err
		}

		cmd, _ := attr.GenerateCommand(store.CommandUpsert)
		return nil, ss.nh.AdvanceWrite(cmd)
	}

	return nil, nil
}

func (ss *RaftSimpleServer) sendResponse(client *clientConn, command string, attr *store.RaftAttribute, err error) {
	client.writeLock.Lock()

	var e error
	var n int

	if err != nil {
		n, e = sendFramedResponse(client.Writer, command, []byte("0"), []byte(err.Error()))
	} else {
		b, _ := attr.Marshal()
		n, e = sendFramedResponse(client.Writer, command, []byte("1"), b)
	}

	if e != nil {
		fmt.Println("sendFramedResponse: ", e)
	}

	fmt.Println("Write num:", n)

	e = client.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if e != nil {
		fmt.Println("SetWriteDeadline: ", e)
	}

	e = client.Writer.Flush()
	if e != nil {
		fmt.Println("Writer Flush: ", e)
	}

	client.writeLock.Unlock()
}

func (ss *RaftSimpleServer) Stop() {
	ss.tcpListener.Close()
}

func sendFramedResponse(w io.Writer, command string, errSignal, b []byte) (int, error) {

	dataSize := make([]byte, 8)

	l := len(b) + 4 + len(command) + 1

	binary.LittleEndian.PutUint64(dataSize, uint64(l))
	if _, err := w.Write(dataSize); err != nil {
		return 0, err
	}

	cmdSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(cmdSize, uint32(len(command)))
	if _, err := w.Write(cmdSize); err != nil {
		return 0, err
	}

	if _, err := w.Write([]byte(command)); err != nil {
		return 0, err
	}

	if _, err := w.Write(errSignal); err != nil {
		return 0, err
	}

	if _, err := w.Write(b); err != nil {
		return 0, err
	}

	return l + 8, nil
}

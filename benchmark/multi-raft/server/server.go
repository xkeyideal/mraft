package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"mraft/store"
	"net"
	"runtime"
	"strings"
	"sync"
)

const defaultBufferSize = 5 * 1024

type SimpleServer struct {
	writelock *sync.Mutex

	tcpListener net.Listener
}

func NewSimpleServer(address string) (*SimpleServer, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	ss := &SimpleServer{
		writelock:   &sync.Mutex{},
		tcpListener: l,
	}

	go ss.TCPServer()

	return ss, nil
}

func (ss *SimpleServer) TCPServer() error {
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
			fmt.Println(err)
			break
		}

		go ss.handle(conn)
	}

	return nil
}

func (ss *SimpleServer) handle(conn net.Conn) error {

	var err error

	reader := bufio.NewReaderSize(conn, defaultBufferSize)
	writer := bufio.NewWriterSize(conn, defaultBufferSize)

	sz := make([]byte, 8)

	for {
		_, err = reader.Read(sz)
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

		_, err = io.ReadFull(reader, body)
		if err != nil {
			err = fmt.Errorf("failed to read databody - %s", err)
			break
		}

		attr := &store.RaftAttribute{}
		err = attr.Unmarshal(body)
		if err != nil {
			err = fmt.Errorf("failed to unmarshal databody - %s", err)
			break
		} else {
			fmt.Printf("%+v\n", attr)
		}

		ss.writelock.Lock()

		wd, e := encode([]byte("done"))
		if e != nil {
			fmt.Println(e)
		}
		n, e := writer.Write(wd)
		fmt.Println("send:", n, e)
		writer.Flush()

		ss.writelock.Unlock()
	}

	return err
}

func (ss *SimpleServer) Stop() {
	ss.tcpListener.Close()
}

func encode(b []byte) ([]byte, error) {
	buf := &bytes.Buffer{}

	dataSize := make([]byte, 8)

	l := len(b)

	binary.LittleEndian.PutUint64(dataSize, uint64(l))
	if _, err := buf.Write(dataSize); err != nil {
		return nil, err
	}

	if _, err := buf.Write(b); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

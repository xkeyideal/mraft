package client

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"mraft/store"
	"net"
	"sync"
)

const defaultBufferSize = 5 * 1024

type SimpleClient struct {
	mu      *sync.Mutex
	message chan *store.RaftAttribute
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
}

func NewSimpleClient(address string) (*SimpleClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	sc := &SimpleClient{
		mu:      &sync.Mutex{},
		message: make(chan *store.RaftAttribute, 1000),
		conn:    conn,
		writer:  bufio.NewWriterSize(conn, defaultBufferSize),
		reader:  bufio.NewReaderSize(conn, defaultBufferSize),
	}

	go sc.handleSend()
	go sc.handleRecv()

	return sc, nil
}

// Flush writes all buffered data to the underlying TCP connection
func (sc *SimpleClient) Flush() error {
	return sc.writer.Flush()
}

func (sc *SimpleClient) Write(b []byte) (int, error) {
	return sc.writer.Write(b)
}

func (sc *SimpleClient) writeCommand(attr *store.RaftAttribute) error {
	sc.mu.Lock()

	_, err := attr.WriteTo(sc)
	if err != nil {
		fmt.Println("Write failed,", err.Error())
		sc.mu.Unlock()
		return err
	}

	sc.Flush()

	sc.mu.Unlock()

	return nil
}

func (sc *SimpleClient) SendMessage(attr *store.RaftAttribute) {
	sc.message <- attr
}

func (sc *SimpleClient) handleSend() {

	for {
		select {
		case msg := <-sc.message:
			err := sc.writeCommand(msg)
			if err != nil {
				fmt.Println("Error to send message because of ", err.Error())
				break
			}
		}
	}
}

func (sc *SimpleClient) handleRecv() {
	var err error
	sz := make([]byte, 8)

	for {
		_, err = sc.reader.Read(sz)
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

		_, err = io.ReadFull(sc.reader, body)
		if err != nil {
			err = fmt.Errorf("failed to read databody - %s", err)
			break
		}

		fmt.Println(string(body))
	}
}

func (sc *SimpleClient) Stop() {
	sc.conn.Close()
}

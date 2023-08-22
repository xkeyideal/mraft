package raft_client

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/xkeyideal/mraft/experiment/store"
)

const defaultBufferSize = 5 * 1024

var ErrStopped = errors.New("stopped")

type ProducerTransaction struct {
	cmd      interface{}
	doneChan chan *ProducerTransaction
	Resp     *store.RaftAttribute
	Error    error
}

type RaftSimpleClient struct {
	mu *sync.Mutex

	conn net.Conn

	reader *bufio.Reader
	writer *bufio.Writer

	responseChan chan []byte

	transactionChan chan *ProducerTransaction
	transactions    []*ProducerTransaction

	exitChan chan struct{}
}

func NewRaftSimpleClient(address string) (*RaftSimpleClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	sc := &RaftSimpleClient{
		mu:              &sync.Mutex{},
		conn:            conn,
		writer:          bufio.NewWriterSize(conn, defaultBufferSize),
		reader:          bufio.NewReaderSize(conn, defaultBufferSize),
		responseChan:    make(chan []byte, 10),
		transactionChan: make(chan *ProducerTransaction),
		exitChan:        make(chan struct{}),
	}

	go sc.handleSend()
	go sc.handleRecv()

	return sc, nil
}

// Flush writes all buffered data to the underlying TCP connection
func (sc *RaftSimpleClient) Flush() error {
	return sc.writer.Flush()
}

func (sc *RaftSimpleClient) Write(b []byte) (int, error) {
	return sc.writer.Write(b)
}

func (sc *RaftSimpleClient) readCommand(arg *store.ReadArgument) error {
	sc.mu.Lock()

	_, err := arg.WriteTo(store.CommandRead, sc)
	if err != nil {
		sc.mu.Unlock()
		return err
	}

	sc.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))

	sc.Flush()

	sc.mu.Unlock()

	return nil
}

func (sc *RaftSimpleClient) writeCommand(attr *store.RaftAttribute) error {
	sc.mu.Lock()

	_, err := attr.WriteTo(store.CommandUpsert, sc)
	if err != nil {
		sc.mu.Unlock()
		return err
	}

	sc.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))

	sc.Flush()

	sc.mu.Unlock()

	return nil
}

func (sc *RaftSimpleClient) PublishCommand(cmd interface{}) (*store.RaftAttribute, error) {
	doneChan := make(chan *ProducerTransaction)
	err := sc.sendCommandAsync(cmd, doneChan)
	if err != nil {
		close(doneChan)
		return nil, err
	}

	// 阻塞
	t := <-doneChan
	return t.Resp, t.Error
}

func (sc *RaftSimpleClient) sendCommandAsync(cmd interface{}, doneChan chan *ProducerTransaction) error {
	t := &ProducerTransaction{
		cmd:      cmd,
		doneChan: doneChan,
	}

	select {
	case sc.transactionChan <- t:
	case <-sc.exitChan:
		return ErrStopped
	}

	return nil
}

func (sc *RaftSimpleClient) popTransaction(data []byte) {
	t := sc.transactions[0]
	sc.transactions = sc.transactions[1:]

	cmdSize := binary.LittleEndian.Uint32(data[:4])
	cmd := string(data[4 : 4+cmdSize])
	errSignal := string(data[4+cmdSize : 4+cmdSize+1])
	switch cmd {
	case store.CommandUpsert:
		if errSignal == "0" {
			t.Error = errors.New(string(data[4+cmdSize+1:]))
		}
	case store.CommandRead:
		if errSignal == "0" {
			t.Error = errors.New(string(data[4+cmdSize+1:]))
		} else {
			attr := &store.RaftAttribute{}
			t.Error = attr.Unmarshal(data[4+cmdSize+1:])
			t.Resp = attr
		}
	}

	t.doneChan <- t
}

func (sc *RaftSimpleClient) handleSend() {
	for {
		select {
		case t := <-sc.transactionChan:
			sc.transactions = append(sc.transactions, t)
			switch t.cmd.(type) {
			case *store.ReadArgument:
				sc.readCommand(t.cmd.(*store.ReadArgument))
			case *store.RaftAttribute:
				sc.writeCommand(t.cmd.(*store.RaftAttribute))
			}
		case data := <-sc.responseChan:
			sc.popTransaction(data)
		case <-sc.exitChan:
			return
		}
	}
}

func (sc *RaftSimpleClient) handleRecv() {
	var err error
	sz := make([]byte, 8)

	for {
		sc.conn.SetReadDeadline(time.Now().Add(6 * time.Second))
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

		sc.responseChan <- body
	}
}

func (sc *RaftSimpleClient) Stop() {
	close(sc.exitChan)
	sc.conn.Close()
}

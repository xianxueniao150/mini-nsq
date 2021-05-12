package nsqd

import (
	"bufio"
	"net"
	"sync"
)

const defaultBufferSize = 16 * 1024

type client struct {
	sync.Mutex

	// original connection
	net.Conn

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	SubEventChan chan *Channel //传递订阅事件

	Channel *Channel
}

func newClient(conn net.Conn) *client {
	c := &client{
		Conn:   conn,
		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),
		//这里有缓存是为了防止处理订阅事件的协程还没准备好订阅事件就来了导致订阅阻塞，
		//因为一个消费者只能订阅一次，所以这里容量为1
		SubEventChan: make(chan *Channel, 1),
	}
	return c
}

func (c *client) Flush() error {
	return c.Writer.Flush()
}

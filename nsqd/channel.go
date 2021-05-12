package nsqd

import (
	"errors"
	"log"
	"sync"
	"time"
)

// Channel represents the concrete type for a NSQ channel
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
type Channel struct {
	topicName     string
	name          string
	memoryMsgChan chan *Message //暂存发送到该channel下的message
	backend       *diskQueue

	inFlightMessages map[MessageID]*Message //存放已发送但客户端还未接收的数据
	inFlightPQ       *inFlightPqueue        //存放已发送但客户端还未接收的数据
	inFlightMutex    sync.Mutex
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string) *Channel {
	c := &Channel{
		topicName:     topicName,
		name:          channelName,
		memoryMsgChan: make(chan *Message, 100),
		backend:       NewDiskQueue(getBackendName(topicName, channelName)),
	}
	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(10000)
	c.inFlightMutex.Unlock()
	return c
}

// PutMessage writes a Message to the queue
func (c *Channel) PutMessage(m *Message) error {
	log.Printf("message 进入 channel,body:%s", m.Body)
	select {
	case c.memoryMsgChan <- m: //如果内存放得下，就先放到内存中
	default:
		err := writeMessageToBackend(m, c.backend) //如果内存放不下，就记录到磁盘里
		if err != nil {
			log.Printf(
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

//每次在向客户端实际发送消息前都会调用
func (c *Channel) StartInFlightTimeout(msg *Message) error {
	now := time.Now()
	msg.pri = now.Add(10 * time.Second).UnixNano()
	return c.pushInFlightMessage(msg)
}

// 如果接收到了客户端的 “FIN” 消息，就会被调用
func (c *Channel) FinishMessage(id MessageID) error {
	_, err := c.popInFlightMessage(id)
	return err
}

// 如果接收到了客户端的 “REQ” 消息，就会被调用
func (c *Channel) RequeueMessage(id MessageID) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(id)
	if err != nil {
		return err
	}
	return c.PutMessage(msg)
}

//每隔固定时间调用一次
func (c *Channel) processInFlightQueue(t int64) bool {
	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg := c.inFlightPQ.WPeek(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		log.Printf("message:%s is dirty", msg.Body)
		dirty = true

		_, err := c.popInFlightMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.PutMessage(msg)
	}

exit:
	return dirty
}

func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()

	//1.加入map
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg

	//2.加入inFlightPQ
	c.inFlightPQ.WPush(msg)
	return nil
}

func (c *Channel) popInFlightMessage(id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()

	//1.从map中删除
	msg, ok := c.inFlightMessages[id]
	if !ok {
		return nil, errors.New("ID not in flight")
	}
	delete(c.inFlightMessages, id)

	//2.从inFlightPQ中删除
	c.inFlightPQ.WRemove(msg)
	return msg, nil
}

func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
	backendName := topicName + "___" + channelName
	return backendName
}

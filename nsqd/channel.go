package nsqd

import (
	"log"
)

// Channel represents the concrete type for a NSQ channel
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
type Channel struct {
	topicName     string
	name          string
	memoryMsgChan chan *Message //暂存发送到该channel下的message
	backend       *diskQueue
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string) *Channel {
	return &Channel{
		topicName:     topicName,
		name:          channelName,
		memoryMsgChan: make(chan *Message, 1),
		backend:       NewDiskQueue(getBackendName(topicName, channelName)),
	}
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

func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
	backendName := topicName + "___" + channelName
	return backendName
}

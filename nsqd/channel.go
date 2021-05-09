package nsqd

import (
	"log"
)

// Channel represents the concrete type for a NSQ channel
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
type Channel struct {
	topicName string
	name      string
	memoryMsgChan chan *Message  //暂存发送到该channel下的message

}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string) *Channel {
	return &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  make(chan *Message, 10000),
	}
}


// PutMessage writes a Message to the queue
func (c *Channel) PutMessage(m *Message) error {
	log.Printf("message 进入 channel,body:%s",m.Body)
	c.memoryMsgChan <- m
	return nil
}




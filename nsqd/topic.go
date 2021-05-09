package nsqd

import (
	"log"
	"sync"
)

type Topic struct {
	name              string

	sync.RWMutex 	//为了保证channelMap的安全
	channelMap        map[string]*Channel //记录该 topic 下的所有 channel

	memoryMsgChan     chan *Message //暂存发送到该 topic 中的 message
	channelUpdateChan chan int  //当该 topic 下的 channel 发生变动时，用于通知
}

// Topic constructor
func NewTopic(topicName string) *Topic {
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, 10000),
		channelUpdateChan: make(chan int),
	}
	go t.messagePump() //开启工作协程
	return t
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
	log.Printf("message 进入 topic")
	t.memoryMsgChan <- m
	return nil
}

// 将 topic 中存的 message 写入该 topic下的 channel 中
func (t *Topic) messagePump() {
	var msg *Message
	var chans []*Channel
	var memoryMsgChan chan *Message

	t.Lock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.Unlock()

	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
	}

	// main message loop
	for {
		select {
		case msg = <-memoryMsgChan:
			//到这里的时候chans的数量必须 >0,否则消息就丢失了,
			//所以我们处理时会在chans为 0的时候将memoryMsgChan置为nil
			for _, channel := range chans {
				err := channel.PutMessage(msg)
				if err != nil {
					log.Printf(
						"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
						t.name, msg.ID, channel.name, err)
				}
			}
		case <-t.channelUpdateChan:
			log.Println("topic 更新 channel")
			chans = chans[:0]
			t.Lock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.Unlock()
			if len(chans) == 0 {
				memoryMsgChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
			}
		}
	}
}


func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()
	if isNew {
		t.channelUpdateChan <- 1
	}
	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		channel = NewChannel(t.name, channelName)
		t.channelMap[channelName] = channel
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}


func (t *Topic) GenerateID() MessageID {
	var h MessageID
	return h
}

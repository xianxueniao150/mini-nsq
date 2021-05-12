package nsqd

import (
	"log"
	"sync"
	"time"
)

type Topic struct {
	name string

	sync.RWMutex                     //为了保证channelMap的安全
	channelMap   map[string]*Channel //记录该 topic 下的所有 channel

	memoryMsgChan     chan *Message //暂存发送到该 topic 中的 message
	channelUpdateChan chan int      //当该 topic 下的 channel 发生变动时，用于通知

	backend   *diskQueue
	idFactory *guidFactory
}

// Topic constructor
func NewTopic(topicName string) *Topic {
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, 100),
		channelUpdateChan: make(chan int),
		idFactory:         NewGUIDFactory(),
	}
	t.backend = NewDiskQueue(topicName)
	go t.messagePump() //开启工作协程
	return t
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
	log.Printf("message 进入 topic")
	select {
	case t.memoryMsgChan <- m: //如果内存放得下，就先放到内存中
	default:
		err := writeMessageToBackend(m, t.backend) //如果内存放不下，就记录到磁盘里
		if err != nil {
			log.Printf(
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}

// 将 topic 中存的 message 写入该 topic下的 channel 中
func (t *Topic) messagePump() {
	var err error
	var msg *Message
	var buf []byte
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte

	t.Lock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.Unlock()

	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// main message loop
	for {
		select {
		case msg = <-memoryMsgChan:
		case buf = <-backendChan:
			msg, err = DecodeMessage(buf)
			if err != nil {
				log.Printf("failed to decode message - %s", err)
				continue
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
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		}

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
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			log.Printf("TOPIC(%s): failed to create guid - %s", t.name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}

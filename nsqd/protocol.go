package nsqd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

var separatorBytes = []byte(" ")

type protocol struct {
	nsqd *NSQD
}


func (p *protocol) NewClient(conn net.Conn) *client {
	return newClient(conn)
}

//每个客户端都有一个对应的工作协程,负责接收来自客户端的消息，并进行实际处理
func (p *protocol) IOLoop(client *client) error {
	var err error
	var line []byte

	//另起一条协程处理消费者相关
	go p.messagePump(client)

	for {
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes)

		err = p.Exec(client, params)
		if err != nil {
			break
		}
	}
	return err
}

//处理来自消费者的订阅消息以及将消息队列中保存的数据发送给消费者
func (p *protocol) messagePump(client *client) {
	var err error
	var memoryMsgChan chan *Message
	var subChannel *Channel
	//这里新创建subEventChan是为了在下面可以把它置为nil以实现“一个客户端只能订阅一次”的目的
	subEventChan := client.SubEventChan

	for {
		select {
		case subChannel = <-subEventChan:  //表示有订阅事件发生,这里的subChannel就是消费者实际绑定的channel
			log.Printf("topic:%s channel:%s 发生订阅事件",subChannel.topicName,subChannel.name)
			memoryMsgChan = subChannel.memoryMsgChan
			// you can't SUB anymore
			subEventChan = nil
		case msg := <-memoryMsgChan: //如果channel对应的内存通道有消息的话
			err = p.SendMessage(client, msg)
			if err != nil {
				log.Printf("PROTOCOL(V2): [%s] messagePump error - %s", client.RemoteAddr(), err)
				goto exit
			}
		}
	}

exit:
	log.Printf("PROTOCOL(V2): [%s] exiting messagePump", client.RemoteAddr())
}

func (p *protocol) Exec(client *client, params [][]byte)  error {
	switch {
	case bytes.Equal(params[0], []byte("PUB")): //Publish a message to a topic
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("SUB")): //Subscribe to a topic/channel
		return p.SUB(client, params)
	}
	return  errors.New(fmt.Sprintf("invalid command %s", params[0]))
}

func (p *protocol) SUB(client *client, params [][]byte)  error {
	topicName := string(params[1])
	channelName := string(params[2])

	var channel *Channel
	topic := p.nsqd.GetTopic(topicName)
	channel = topic.GetChannel(channelName)
	// update message pump
	client.SubEventChan <- channel

	return nil
}

func (p *protocol) PUB(client *client, params [][]byte)  error {
	var err error
	topicName := string(params[1])
	messageLen := make([]byte,4)
	_, err  = io.ReadFull(client.Reader, messageLen)
	if err != nil {
		return err
	}
	bodyLen:= int32(binary.BigEndian.Uint32(messageLen))
	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return err
	}

	topic := p.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	log.Printf("receive message from %s, topic:%s, message: %s",client.RemoteAddr(),topicName,string(messageBody))
	_ = topic.PutMessage(msg)
	return nil
}

func (p *protocol) SendMessage(client *client, msg *Message) error {
	log.Printf("PROTOCOL(V2): writing to client(%s) - message: %s", client.RemoteAddr(), msg.Body)

	msgByte, err := msg.Bytes()
	if err != nil {
		return err
	}
	return p.Send(client, msgByte)
}

func (p *protocol) Send(client *client,data []byte) error {
	client.Lock()
	defer client.Unlock()
	_, err := SendFramedResponse(client.Writer, data)
	if err != nil {
		return err
	}
	//因为client.Writer使用了bufio缓存，所以这里我们就先暂时强制刷新
	err = client.Flush()
	return err
}

// 进行实际发送，并且会在消息前面加上4bytes的消息长度
func SendFramedResponse(w io.Writer, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data))

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	n, err = w.Write(data)
	return n + 4, err
}


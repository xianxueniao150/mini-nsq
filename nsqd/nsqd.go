package nsqd

import (
	"fmt"
	"log"
	"net"
	"sync"
)


type NSQD struct {
	sync.RWMutex
	topicMap map[string]*Topic
}

func Start() (*NSQD, error) {
	var err error
	var tcpListener net.Listener
	tcpListener, err = net.Listen("tcp", "0.0.0.0:4150")
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", "0.0.0.0:4150", err)
	}
	log.Printf("TCP: listening on %s", tcpListener.Addr())

	n := &NSQD{
		topicMap:             make(map[string]*Topic),
	}
	tcpServer := &tcpServer{nsqd: n,tcpListener: tcpListener}
	go tcpServer.serve()
	return n, nil
}


// GetTopic performs a thread safe operation
// 没有就新建
func (n *NSQD) GetTopic(topicName string) *Topic {
	n.Lock()
	defer n.Unlock()
	t, ok := n.topicMap[topicName]
	if ok {
		return t
	}
	t = NewTopic(topicName)
	n.topicMap[topicName] = t
	return t
}

// channels returns a flat slice of all channels in all topics
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}


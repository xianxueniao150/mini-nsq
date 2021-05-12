package nsqd

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
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
		topicMap: make(map[string]*Topic),
	}
	tcpServer := &tcpServer{nsqd: n, tcpListener: tcpListener}
	go tcpServer.serve()
	go n.queueScanLoop()
	return n, nil
}

func (n *NSQD) queueScanLoop() {
	workTicker := time.NewTicker(100 * time.Millisecond)
	defer workTicker.Stop()
	var channels []*Channel
	for {
		select {
		case <-workTicker.C:
			channels = n.channels()
			if len(channels) == 0 {
				continue
			}
		}

		num := 20
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		numDirty := 0
		for _, i := range UniqRands(num, len(channels)) {
			c := channels[i]
			now := time.Now().UnixNano()
			if c.processInFlightQueue(now) {
				numDirty++
			}
		}
		// If 25% of the selected channels were dirty,
		// the loop continues without sleep.
		if float64(numDirty)/float64(num) > 0.25 {
			goto loop
		}
	}
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

// examplae: input (3,10) output: [0 8 7]
func UniqRands(quantity int, maxval int) []int {
	if maxval < quantity {
		quantity = maxval
	}

	intSlice := make([]int, maxval)
	for i := 0; i < maxval; i++ {
		intSlice[i] = i
	}

	for i := 0; i < quantity; i++ {
		j := rand.Int()%maxval + i
		// swap
		intSlice[i], intSlice[j] = intSlice[j], intSlice[i]
		maxval--

	}
	return intSlice[0:quantity]
}

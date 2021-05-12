package nsqd

import "container/heap"

type inFlightPqueue []*Message

func newInFlightPqueue(capacity int) *inFlightPqueue {
	pq := make(inFlightPqueue, 0, capacity)
	return &pq
}

//以下 5个方法是 heap 规定的要实现优先级队列必须实现的 5个方法
func (pq inFlightPqueue) Len() int { return len(pq) }

func (pq inFlightPqueue) Less(i, j int) bool {
	return pq[i].pri < pq[j].pri
}

func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *inFlightPqueue) Push(x interface{}) {
	msg := x.(*Message)
	n := len(*pq)
	msg.index = n
	*pq = append(*pq, msg)
}

func (pq *inFlightPqueue) Pop() interface{} {
	n := len(*pq)
	msg := (*pq)[n-1]
	*pq = (*pq)[0 : n-1]
	msg.index = -1
	return msg
}

//以下 “W” 开头的方法是我们提供给外部使用的
//向队列中加入1条message
func (pq *inFlightPqueue) WPush(msg *Message) {
	heap.Push(pq, msg)
}

//从队列中弹出最早过期的那条 message
func (pq *inFlightPqueue) WPop() *Message {
	return heap.Pop(pq).(*Message)
}

//从队列中删除指定的 message
func (pq *inFlightPqueue) WRemove(msg *Message) {
	heap.Remove(pq, msg.index)
}

//查找队列中最早过期的那条 message，但并不弹出
func (pq *inFlightPqueue) WPeek(max int64) *Message {
	if len(*pq) == 0 {
		return nil
	}
	msg := (*pq)[0]
	if msg.pri > max {
		return nil
	}
	return msg
}

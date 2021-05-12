package nsqd

import (
	"fmt"
	"testing"
)

func TestInFlightPqueue(t *testing.T) {
	pq := newInFlightPqueue(5)
	//var id MessageID
	msg1 := &Message{
		pri:  1,
		Body: []byte("first"),
	}
	pq.WPush(msg1)

	msg2 := &Message{
		pri:  3,
		Body: []byte("three"),
	}
	pq.WPush(msg2)

	msg3 := &Message{
		pri:  2,
		Body: []byte("two"),
	}
	pq.WPush(msg3)

	pq.WRemove(msg1)

	for len(*pq) > 0 {
		fmt.Printf("%s\n", pq.WPop().Body)
	}
}

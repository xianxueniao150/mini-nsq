package main

import (
	"encoding/binary"
	"fmt"
	"github.com/bowen/mynsq/nsqd"
	"log"
	"net"
	"reflect"
	"time"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ltime)
	nsqdAddr := "127.0.0.1:4150"
	conn, err := net.Dial("tcp", nsqdAddr)
	go readFully(conn)
	if err != nil {
		log.Fatal(err)
	}
	pub(conn, "mytopic", []byte("one one "))
	pub(conn, "mytopic", []byte("two two"))
	pub(conn, "mytopic", []byte("three three"))

	cmd := Subscribe("mytopic", "mychannel")
	cmd.WriteTo(conn)

	select {}
}

func readFully(conn net.Conn) {
	len := make([]byte, 4)
	retry := 0
	for {
		_, err := conn.Read(len)
		if err != nil {
			fmt.Printf("error during read: %s", err)
		}
		size := binary.BigEndian.Uint32(len)
		data := make([]byte, size)
		var n int
		n, err = conn.Read(data)
		if err != nil {
			fmt.Printf("error during read: %s", err)
		}
		msg, _ := nsqd.DecodeMessage(data)
		log.Printf("local:%s, receive: id:<%v> body:<%s>,size:%d\n", conn.LocalAddr(), msg.ID, msg.Body, n)
		if reflect.DeepEqual(msg.Body, []byte("two two")) && retry < 3 {
			retry++
			requeue(conn, msg.ID)
			log.Printf("requeue message success -- msgID: %s", msg.Body)
			time.Sleep(time.Second)
		}
		if reflect.DeepEqual(msg.Body, []byte("three three")) {
			finish(conn, msg.ID)
			log.Printf("finish message success -- msgID: %s", msg.Body)
		}
	}
}

func pub(conn net.Conn, topic string, msg []byte) {
	cmd := Publish(topic, msg)
	cmd.WriteTo(conn)
}

func requeue(conn net.Conn, id nsqd.MessageID) {
	cmd := Requeue(id)
	cmd.WriteTo(conn)
}

func finish(conn net.Conn, id nsqd.MessageID) {
	cmd := Finish(id)
	cmd.WriteTo(conn)
}

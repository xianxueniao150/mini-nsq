package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
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
	var cmd *Command
	pubOnce(conn)
	time.Sleep(time.Second * 3)

	cmd = Subscribe("mytopic", "mychannel")
	cmd.WriteTo(conn)

	select {}
}

func pubOnce(conn net.Conn) {
	var cmd *Command
	cmd = Publish("mytopic", []byte("one one "))
	cmd.WriteTo(conn)

	cmd = Publish("mytopic", []byte("two two"))
	cmd.WriteTo(conn)

	cmd = Publish("mytopic", []byte("three three"))
	cmd.WriteTo(conn)

	cmd = Publish("mytopic", []byte("four four"))
	cmd.WriteTo(conn)
}

func readFully(conn net.Conn) {
	len := make([]byte, 4)
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
		log.Printf("local:%s, receive: <%s> ,size:%d\n", conn.LocalAddr(), data[16:n], n)
	}
}

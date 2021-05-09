package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

func main() {
	nsqdAddr := "127.0.0.1:4150"
	conn, err := net.Dial("tcp", nsqdAddr)
	go readFully(conn)
	if err != nil {
		log.Fatal(err)
	}
	cmd := Publish("mytopic", []byte("ha ha"))
	cmd.WriteTo(conn)

	cmd = Subscribe("mytopic", "mychannel")
	cmd.WriteTo(conn)

	select {

	}
}

func readFully(conn net.Conn) {
	len:=make([]byte, 4)
	for {
		_, err := conn.Read(len)
		if err != nil {
			fmt.Printf("error during read: %s", err)
		}
		size :=binary.BigEndian.Uint32(len)
		data := make([]byte, size)
		var n int
		n, err = conn.Read(data)
		if err != nil {
			fmt.Printf("error during read: %s", err)
		}
		fmt.Printf("receive: <%s> ,size:%d\n", data[16:n],n)
	}
}

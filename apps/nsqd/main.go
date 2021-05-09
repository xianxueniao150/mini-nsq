package main

import (
	"github.com/bowen/mynsq/nsqd"
	"log"
)


func main() {
	log.SetFlags(log.Lshortfile | log.Ltime)
	_, err := nsqd.Start()
	if err != nil {
		log.Printf("failed to instantiate nsqd - %s", err)
	}
	select {
	}
}


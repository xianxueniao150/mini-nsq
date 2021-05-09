package main

import (
	"encoding/binary"
	"io"
)

var byteSpace = []byte(" ")
var byteNewLine = []byte("\n")

// Command represents a command from a client to an NSQ daemon
type Command struct {
	Name   []byte
	Params [][]byte
	Body   []byte
}



// WriteTo implements the WriterTo interface and
// serializes the Command to the supplied Writer.
//
// It is suggested that the target Writer is buffered
// to avoid performing many system calls.
func (c *Command) WriteTo(w io.Writer) (int64, error) {
	var total int64
	var buf [4]byte

	n, err := w.Write(c.Name)
	total += int64(n)
	if err != nil {
		return total, err
	}

	for _, param := range c.Params {
		n, err := w.Write(byteSpace)
		total += int64(n)
		if err != nil {
			return total, err
		}
		n, err = w.Write(param)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	n, err = w.Write(byteNewLine)
	total += int64(n)
	if err != nil {
		return total, err
	}

	if c.Body != nil {
		bufs := buf[:]
		binary.BigEndian.PutUint32(bufs, uint32(len(c.Body)))
		n, err := w.Write(bufs)
		total += int64(n)
		if err != nil {
			return total, err
		}
		n, err = w.Write(c.Body)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// Publish creates a new Command to write a message to a given topic
func Publish(topic string, body []byte) *Command {
	var params = [][]byte{[]byte(topic)}
	return &Command{[]byte("PUB"), params, body}
}


// Subscribe creates a new Command to subscribe to the given topic/channel
func Subscribe(topic string, channel string) *Command {
	var params = [][]byte{[]byte(topic), []byte(channel)}
	return &Command{[]byte("SUB"), params, nil}
}


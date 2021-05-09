package nsqd

import (
	"bytes"
)

const (
	MsgIDLength       = 16
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
	}
}

func (m *Message) Bytes() ([]byte,error) {
	buf := new(bytes.Buffer)
	_, err := buf.Write(m.ID[:])
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(m.Body)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}



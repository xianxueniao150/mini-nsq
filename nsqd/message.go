package nsqd

import (
	"bytes"
)

const (
	MsgIDLength = 16
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID   MessageID
	Body []byte
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:   id,
		Body: body,
	}
}

func (m *Message) Bytes() ([]byte, error) {
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

func decodeMessage(b []byte) (*Message, error) {
	var msg Message
	copy(msg.ID[:], b[:MsgIDLength])
	msg.Body = b[MsgIDLength:]
	return &msg, nil
}

func writeMessageToBackend(msg *Message, bq *diskQueue) error {
	msgByte, err := msg.Bytes()
	if err != nil {
		return err
	}
	return bq.Put(msgByte)
}

package message

// ByteMessage implements substrate.DiscardableMessage interface by
// returning the payload on a call to the data method.
type ByteMessage struct {
	Payload []byte
}

// NewByteMessage returns a new instance of byte message.
func NewByteMessage(payload []byte) *ByteMessage {
	return &ByteMessage{Payload: payload}
}

// Data returns the payload.
func (msg *ByteMessage) Data() []byte {
	return msg.Payload
}

// DiscardPayload discards the payload.
func (msg *ByteMessage) DiscardPayload() {
	msg.Payload = nil
}

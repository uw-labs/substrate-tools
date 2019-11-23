package message

// Message implements substrate.DiscardableMessage interface by
// returning the payload on a call to the data method.
type Message struct {
	Payload []byte
}

// NewMessage returns a new instance of message.
func NewMessage(payload []byte) *Message {
	return &Message{Payload: payload}
}

// FromString convenience method to initialise a byte message from string.
func FromString(payload string) *Message {
	return &Message{Payload: []byte(payload)}
}

// Data returns the payload.
func (msg *Message) Data() []byte {
	return msg.Payload
}

// DiscardPayload discards the payload.
func (msg *Message) DiscardPayload() {
	msg.Payload = nil
}

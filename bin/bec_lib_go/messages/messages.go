package messages

type BECMessage interface {
	becMessage()
}

type VariableMessage struct {
	Metadata map[string]string `msgpack:"metadata" json:"metadata"`
	Value    interface{}       `msgpack:"value" json:"value"`
}

type ClientRestartMessage struct {
	Metadata map[string]string `msgpack:"metadata" json:"metadata"`
	Reason   string            `msgpack:"reason" json:"reason"`
}

func (VariableMessage) becMessage() {}

func (ClientRestartMessage) becMessage() {}

func init() {
	registerDecoder[VariableMessage]("VariableMessage")
	registerDecoder[ClientRestartMessage]("ClientRestartMessage")
}

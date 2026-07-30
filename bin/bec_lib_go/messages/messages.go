package messages

import "github.com/vmihailenco/msgpack/v5"

type CodecWrapper[T any] struct {
	BecCodec CodecData[T] `msgpack:"__bec_codec__" json:"__bec_codec__"`
}

type CodecData[T any] struct {
	EncoderName string `msgpack:"encoder_name" json:"encoder_name"`
	TypeName    string `msgpack:"type_name" json:"type_name"`
	Data        T      `msgpack:"data" json:"data"`
}

type VariableMessagePayload struct {
	MsgType  string            `msgpack:"msg_type" json:"msg_type"`
	Value    interface{}       `msgpack:"value" json:"value"`
	Metadata map[string]string `msgpack:"metadata" json:"metadata"`
}

type ClientRestartMessagePayload struct {
	Reason   string            `msgpack:"reason" json:"reason"`
	Metadata map[string]string `msgpack:"metadata" json:"metadata"`
}

func NewVariableMessage(value interface{}, metadata map[string]string) CodecWrapper[VariableMessagePayload] {
	return CodecWrapper[VariableMessagePayload]{
		BecCodec: CodecData[VariableMessagePayload]{
			EncoderName: "BECMessage",
			TypeName:    "VariableMessage",
			Data: VariableMessagePayload{
				MsgType:  "var_message",
				Value:    value,
				Metadata: ensureMetadata(metadata),
			},
		},
	}
}

func NewClientRestartMessage(reason string, metadata map[string]string) CodecWrapper[ClientRestartMessagePayload] {
	return CodecWrapper[ClientRestartMessagePayload]{
		BecCodec: CodecData[ClientRestartMessagePayload]{
			EncoderName: "BECMessage",
			TypeName:    "ClientRestartMessage",
			Data: ClientRestartMessagePayload{
				Reason:   reason,
				Metadata: ensureMetadata(metadata),
			},
		},
	}
}

func Encode[T any](message CodecWrapper[T]) ([]byte, error) {
	return msgpack.Marshal(message)
}

func DecodeVariableMessage(data []byte) (VariableMessagePayload, error) {
	var decoded CodecWrapper[VariableMessagePayload]
	if err := msgpack.Unmarshal(data, &decoded); err != nil {
		return VariableMessagePayload{}, err
	}
	return decoded.BecCodec.Data, nil
}

func ensureMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	return metadata
}

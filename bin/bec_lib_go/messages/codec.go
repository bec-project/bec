package messages

import (
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

type CodecWrapper[T any] struct {
	BecCodec CodecData[T] `msgpack:"__bec_codec__" json:"__bec_codec__"`
}

type CodecData[T any] struct {
	EncoderName string `msgpack:"encoder_name" json:"encoder_name"`
	TypeName    string `msgpack:"type_name" json:"type_name"`
	Data        T      `msgpack:"data" json:"data"`
}

type codecHeader struct {
	BecCodec codecHeaderData `msgpack:"__bec_codec__" json:"__bec_codec__"`
}

type codecHeaderData struct {
	TypeName string `msgpack:"type_name" json:"type_name"`
}

var decoders = make(map[string]func([]byte) (BECMessage, error))

func Encode(message BECMessage) ([]byte, error) {
	return msgpack.Marshal(CodecWrapper[BECMessage]{
		BecCodec: CodecData[BECMessage]{
			EncoderName: "BECMessage",
			TypeName:    typeName(message),
			Data:        message,
		},
	})
}

func Decode(data []byte) (BECMessage, error) {
	var header codecHeader
	if err := msgpack.Unmarshal(data, &header); err != nil {
		return nil, err
	}

	decode, ok := decoders[header.BecCodec.TypeName]
	if !ok {
		return nil, fmt.Errorf("unsupported message type: %s", header.BecCodec.TypeName)
	}
	return decode(data)
}

func typeName(message any) string {
	typ := reflect.TypeOf(message)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ.Name()
}

func registerDecoder[T BECMessage](typeName string) {
	decoders[typeName] = func(data []byte) (BECMessage, error) {
		var decoded CodecWrapper[T]
		if err := msgpack.Unmarshal(data, &decoded); err != nil {
			return nil, err
		}
		return decoded.BecCodec.Data, nil
	}
}

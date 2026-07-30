package endpoints

import (
	"reflect"

	"bec_lib_go/messages"
)

type RedisOperation string

const (
	Send   RedisOperation = "send"
	Stream RedisOperation = "stream"
)

type EndpointInfo struct {
	Topic          string
	RedisOperation RedisOperation
	MessageType    string
}

var (
	Account       = newEndpointInfo[messages.VariableMessage]("info/account", Stream)
	ClientRestart = newEndpointInfo[messages.ClientRestartMessage]("info/client_restart", Send)
)

func newEndpointInfo[T any](topic string, operation RedisOperation) EndpointInfo {
	var zero T

	return EndpointInfo{
		Topic:          topic,
		RedisOperation: operation,
		MessageType:    typeName(zero),
	}
}

// typeName returns the name of the type of the provided message.
func typeName(message any) string {
	typ := reflect.TypeOf(message)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ.Name()
}

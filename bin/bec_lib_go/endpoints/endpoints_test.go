package endpoints

import "testing"

func TestAccountEndpoint(t *testing.T) {
	t.Parallel()

	if Account.Topic != "info/account" {
		t.Fatalf("Account.Topic = %q, want %q", Account.Topic, "info/account")
	}
	if Account.RedisOperation != Stream {
		t.Fatalf("Account.RedisOperation = %q, want %q", Account.RedisOperation, Stream)
	}
	if Account.MessageType != "VariableMessage" {
		t.Fatalf("Account.MessageType = %q, want %q", Account.MessageType, "VariableMessage")
	}
}

func TestClientRestartEndpoint(t *testing.T) {
	t.Parallel()

	if ClientRestart.Topic != "info/client_restart" {
		t.Fatalf("ClientRestart.Topic = %q, want %q", ClientRestart.Topic, "info/client_restart")
	}
	if ClientRestart.RedisOperation != Send {
		t.Fatalf("ClientRestart.RedisOperation = %q, want %q", ClientRestart.RedisOperation, Send)
	}
	if ClientRestart.MessageType != "ClientRestartMessage" {
		t.Fatalf("ClientRestart.MessageType = %q, want %q", ClientRestart.MessageType, "ClientRestartMessage")
	}
}

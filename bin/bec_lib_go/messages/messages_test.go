package messages

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestEncodeInfersTypeNameFromMessage(t *testing.T) {
	t.Parallel()

	encoded, err := Encode(ClientRestartMessage{
		Reason:   "maintenance",
		Metadata: map[string]string{},
	})
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	var decoded CodecWrapper[ClientRestartMessage]
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("msgpack.Unmarshal returned error: %v", err)
	}

	if decoded.BecCodec.TypeName != "ClientRestartMessage" {
		t.Fatalf("TypeName = %q, want %q", decoded.BecCodec.TypeName, "ClientRestartMessage")
	}

	if decoded.BecCodec.Data.Reason != "maintenance" {
		t.Fatalf("Reason = %q, want %q", decoded.BecCodec.Data.Reason, "maintenance")
	}
}

func TestDecodePythonFixtures(t *testing.T) {
	t.Parallel()

	testCases := loadFixtureManifest(t)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(filepath.Join(testdataDir(t), "generated", tc.filename))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}

			got, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode returned error: %v", err)
			}

			if !reflect.DeepEqual(got, tc.message) {
				t.Fatalf("Decode = %#v, want %#v", got, tc.message)
			}
		})
	}
}

func TestEncodeMatchesPythonFixtures(t *testing.T) {
	t.Parallel()

	testCases := loadFixtureManifest(t)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want, err := os.ReadFile(filepath.Join(testdataDir(t), "generated", tc.filename))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}

			got, err := Encode(tc.message)
			if err != nil {
				t.Fatalf("Encode returned error: %v", err)
			}

			if !bytes.Equal(got, want) {
				t.Fatalf("encoded bytes differ from Python fixture")
			}
		})
	}
}

func testdataDir(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(filename), "testdata")
}

type fixtureManifestEntry struct {
	Name     string            `json:"name"`
	Filename string            `json:"filename"`
	Type     string            `json:"type"`
	Value    string            `json:"value"`
	Reason   string            `json:"reason"`
	Metadata map[string]string `json:"metadata"`
}

type fixtureTestCase struct {
	name     string
	filename string
	message  BECMessage
}

func loadFixtureManifest(t *testing.T) []fixtureTestCase {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(testdataDir(t), "messages.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	var entries []fixtureManifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}

	out := make([]fixtureTestCase, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name
		if name == "" {
			name = entry.Filename
		}

		var message BECMessage
		switch entry.Type {
		case "VariableMessage":
			message = VariableMessage{
				Value:    entry.Value,
				Metadata: entry.Metadata,
			}
		case "ClientRestartMessage":
			message = ClientRestartMessage{
				Reason:   entry.Reason,
				Metadata: entry.Metadata,
			}
		default:
			t.Fatalf("unsupported manifest message type: %s", entry.Type)
		}

		out = append(out, fixtureTestCase{
			name:     name,
			filename: entry.Filename,
			message:  message,
		})
	}

	return out
}

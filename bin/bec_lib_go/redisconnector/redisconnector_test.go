package redisconnector

import (
	"strings"
	"testing"

	"bec_lib_go/endpoints"

	"github.com/redis/go-redis/v9"
)

func TestExtractStreamData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value interface{}
		want  []byte
	}{
		{
			name:  "string",
			value: "payload",
			want:  []byte("payload"),
		},
		{
			name:  "bytes",
			value: []byte("payload"),
			want:  []byte("payload"),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := extractStreamData(tc.value)
			if err != nil {
				t.Fatalf("extractStreamData returned error: %v", err)
			}
			if string(got) != string(tc.want) {
				t.Fatalf("extractStreamData = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExtractStreamDataRejectsUnexpectedType(t *testing.T) {
	t.Parallel()

	if _, err := extractStreamData(42); err == nil {
		t.Fatal("extractStreamData should reject unexpected value types")
	}
}

func TestNormalizeStreamMessage(t *testing.T) {
	t.Parallel()

	got, err := normalizeStreamMessage(redis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"data": "payload"},
	})
	if err != nil {
		t.Fatalf("normalizeStreamMessage returned error: %v", err)
	}
	if string(got.Data) != "payload" {
		t.Fatalf("normalizeStreamMessage data = %q, want %q", got.Data, "payload")
	}
}

func TestNormalizeStreamMessageMissingDataField(t *testing.T) {
	t.Parallel()

	_, err := normalizeStreamMessage(redis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{},
	})
	if err == nil {
		t.Fatal("normalizeStreamMessage should reject messages without a data field")
	}
	if !strings.Contains(err.Error(), `stream message "1-0" is missing the "data" field`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeStreamMessageWrapsInvalidDataField(t *testing.T) {
	t.Parallel()

	_, err := normalizeStreamMessage(redis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"data": 42},
	})
	if err == nil {
		t.Fatal("normalizeStreamMessage should reject invalid data field types")
	}
	if !strings.Contains(err.Error(), `stream message "1-0" has invalid data field`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTopicForOperation(t *testing.T) {
	t.Parallel()

	topic, err := topicForOperation(endpoints.Account, endpoints.Stream)
	if err != nil {
		t.Fatalf("topicForOperation returned error: %v", err)
	}
	if topic != "info/account" {
		t.Fatalf("topicForOperation = %q, want %q", topic, "info/account")
	}
}

func TestTopicForOperationRejectsUnexpectedOperation(t *testing.T) {
	t.Parallel()

	_, err := topicForOperation(endpoints.ClientRestart, endpoints.Stream)
	if err == nil {
		t.Fatal("topicForOperation should reject mismatched endpoint operations")
	}
	if !strings.Contains(err.Error(), `endpoint "info/client_restart" uses redis operation "send", expected "stream"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

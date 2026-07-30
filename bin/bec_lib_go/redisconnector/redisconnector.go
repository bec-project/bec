package redisconnector

import (
	"context"
	"fmt"

	"bec_lib_go/endpoints"
	"bec_lib_go/messages"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	raw *redis.Client
}

// StreamMessage is the normalized payload extracted from a Redis stream entry.
type StreamMessage struct {
	// Data contains the raw msgpack-encoded BEC message stored in the entry.
	Data []byte
}

// New creates a Redis client without explicit ACL credentials.
func New(host string, port int) *Client {
	return NewWithAuth(host, port, "", "")
}

// NewWithAuth creates a Redis client for host and port using the provided ACL credentials.
func NewWithAuth(host string, port int, username string, password string) *Client {
	return &Client{
		raw: redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", host, port),
			Username: username,
			Password: password,
		}),
	}
}

// Close closes the underlying Redis client.
func (c *Client) Close() error {
	return c.raw.Close()
}

// Ping reports whether the Redis server is reachable with the current client configuration.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.raw.Ping(ctx).Result()
	return err
}

// Publish encodes message and publishes it to endpoint.
// endpoint must describe a send-style Redis operation.
func (c *Client) Publish(
	ctx context.Context, endpoint endpoints.EndpointInfo, message messages.BECMessage,
) error {
	topic, err := topicForOperation(endpoint, endpoints.Send)
	if err != nil {
		return err
	}

	payload, err := messages.Encode(message)
	if err != nil {
		return err
	}
	return c.raw.Publish(ctx, topic, payload).Err()
}

// AddStreamMessage encodes message and appends it to endpoint.
// endpoint must describe a stream-style Redis operation. maxLen and approx are passed
// through to Redis stream trimming.
func (c *Client) AddStreamMessage(
	ctx context.Context,
	endpoint endpoints.EndpointInfo,
	message messages.BECMessage,
	maxLen int64,
	approx bool,
) error {
	stream, err := topicForOperation(endpoint, endpoints.Stream)
	if err != nil {
		return err
	}

	payload, err := messages.Encode(message)
	if err != nil {
		return err
	}
	return c.raw.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"data": payload},
		MaxLen: maxLen,
		Approx: approx,
	}).Err()
}

// XRange reads all entries from endpoint and returns their normalized payloads.
// endpoint must describe a stream-style Redis operation.
func (c *Client) XRange(ctx context.Context, endpoint endpoints.EndpointInfo) ([]StreamMessage, error) {
	stream, err := topicForOperation(endpoint, endpoints.Stream)
	if err != nil {
		return nil, err
	}

	result, err := c.raw.XRange(ctx, stream, "-", "+").Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	streamMessages := make([]StreamMessage, 0, len(result))
	for _, message := range result {
		streamMessage, err := normalizeStreamMessage(message)
		if err != nil {
			return nil, err
		}
		streamMessages = append(streamMessages, streamMessage)
	}

	return streamMessages, nil
}

// normalizeStreamMessage extracts the encoded BEC message payload from a Redis stream entry.
func normalizeStreamMessage(message redis.XMessage) (StreamMessage, error) {
	value, ok := message.Values["data"]
	if !ok {
		return StreamMessage{}, fmt.Errorf(`stream message %q is missing the "data" field`, message.ID)
	}

	data, err := extractStreamData(value)
	if err != nil {
		return StreamMessage{}, fmt.Errorf("stream message %q has invalid data field: %w", message.ID, err)
	}

	return StreamMessage{Data: data}, nil
}

// extractStreamData converts Redis stream field values into raw byte payloads.
func extractStreamData(value interface{}) ([]byte, error) {
	switch data := value.(type) {
	case string:
		return []byte(data), nil
	case []byte:
		return data, nil
	default:
		return nil, fmt.Errorf("unexpected stream data type: %T", value)
	}
}

// topicForOperation verifies that endpoint supports the requested Redis operation.
func topicForOperation(
	endpoint endpoints.EndpointInfo, expectedOperation endpoints.RedisOperation,
) (string, error) {
	if endpoint.RedisOperation != expectedOperation {
		return "", fmt.Errorf(
			"endpoint %q uses redis operation %q, expected %q",
			endpoint.Topic,
			endpoint.RedisOperation,
			expectedOperation,
		)
	}

	return endpoint.Topic, nil
}

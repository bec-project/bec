package redisconnector

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	raw *redis.Client
}

func New(host string, port int) *Client {
	return &Client{
		raw: redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%d", host, port),
		}),
	}
}

func (c *Client) Close() error {
	return c.raw.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.raw.Ping(ctx).Result()
	return err
}

func (c *Client) Publish(ctx context.Context, topic string, payload []byte) error {
	return c.raw.Publish(ctx, topic, payload).Err()
}

func (c *Client) AddStreamMessage(
	ctx context.Context, stream string, payload []byte, maxLen int64, approx bool,
) error {
	return c.raw.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"data": payload},
		MaxLen: maxLen,
		Approx: approx,
	}).Err()
}

func (c *Client) XRange(ctx context.Context, stream string) ([]redis.XMessage, error) {
	return c.raw.XRange(ctx, stream, "-", "+").Result()
}

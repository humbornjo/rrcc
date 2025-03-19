package rrcc

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/event"
)

const (
	ADD  = event.ADD  // Key is updated to a exact value from nil
	CHG  = event.CHG  // Key changed to a new value from a previous value
	DEL  = event.DEL  // Key is either deleted or expired
	PING = event.PING // Key is still alive
)

type Event = event.Event

func FromOptions(ctx context.Context, options redis.Options, opts ...optionClient) (*hub, error) {
	redisClient := redis.NewClient(&options)
	return initHub(ctx, func() *redis.Client { return redisClient })
}

func FromGetConn(ctx context.Context, fn func() *redis.Client, opts ...optionClient) (*hub, error) {
	if fn() == nil {
		return nil, ErrNilConn
	}
	return initHub(ctx, fn)
}

type optionsClient struct {
	prefix string
}
type optionClient func(*optionsClient)

var defaultOptionsClient = optionsClient{
	prefix: "rrcc",
}

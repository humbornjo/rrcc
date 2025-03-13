package rrcc

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/event"
)

const (
	ADD = event.ADD
	CHG = event.CHG
	DEL = event.DEL
)

type Event = event.Event

type Client interface {
	Stop()
	Data(string, ...optionPoll) poller
	// Bind(string, any, ...optionPoll) poller

	watch(context.Context)
	update(string, event.Event, func() string) (Event, error)
}

func FromOptions(ctx context.Context, options redis.Options, opts ...optionClient) (Client, error) {
	redisClient := redis.NewClient(&options)
	return clientInit(ctx, func() *redis.Client { return redisClient })
}

func FromGetConn(ctx context.Context, fn func() *redis.Client, opts ...optionClient) (Client, error) {
	if fn() == nil {
		return nil, ErrNilConn
	}
	return clientInit(ctx, fn)
}

type optionsClient struct {
	prefix string
}
type optionClient func(*optionsClient)

var defaultOptionsClient = optionsClient{
	prefix: "rrcc",
}

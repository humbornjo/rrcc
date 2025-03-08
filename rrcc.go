package rrcc

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Client interface {
	Stop()
	String(string, ...optionPoll) poller
	// Bind(string, any, ...optionPoll) poller

	watch(context.Context)
	update(string, event, func() string) (event, error)
}

func FromOptions(ctx context.Context, opts redis.Options, prefix string) (Client, error) {
	redisClient := redis.NewClient(&opts)
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return initClient(ctx, func() *redis.Client { return redisClient }), nil
}

func FromGetConn(ctx context.Context, getConn func() *redis.Client, prefix string) (Client, error) {
	if getConn() == nil {
		return nil, ErrNilConn
	}

	if err := getConn().Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return initClient(ctx, getConn), nil
}

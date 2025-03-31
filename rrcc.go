package rrcc

import (
	"context"
	"time"

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

func FromOptions(ctx context.Context, options redis.Options, opts ...hubOption) (*hub, error) {
	redisClient := redis.NewClient(&options)
	return initHub(ctx, func() *redis.Client { return redisClient }, opts...)
}

func FromGetConn(ctx context.Context, fn func() *redis.Client, opts ...hubOption) (*hub, error) {
	if fn() == nil {
		return nil, ErrNilConn
	}
	return initHub(ctx, fn, opts...)
}

type hubConfig struct {
	prefix        string
	maxCacheSize  int
	watchInterval time.Duration
	updateTimeout time.Duration
}

type hubOption func(*hubConfig)

func defaultHubConfig() hubConfig {
	return hubConfig{
		prefix:        "rrcc",
		maxCacheSize:  64,
		updateTimeout: 5 * time.Second,
		watchInterval: 10 * time.Second,
	}
}

func WithRedisKeyPrefix(prefix string) hubOption {
	return func(cfg *hubConfig) {
		cfg.prefix = prefix
	}
}

func WithUpdateTimeout(timeout time.Duration) hubOption {
	return func(cfg *hubConfig) {
		cfg.updateTimeout = timeout
	}
}

func WithWatchInterval(interval time.Duration) hubOption {
	return func(cfg *hubConfig) {
		cfg.watchInterval = interval
	}
}

func WithMaxCacheSize(size int) hubOption {
	return func(cfg *hubConfig) {
		cfg.maxCacheSize = size
	}
}

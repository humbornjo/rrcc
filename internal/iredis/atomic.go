package iredis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type rconfig struct {
	Value   string
	Version uint64
}

type WrappedRedis struct {
	GetConn func() *redis.Client
}

func (p *WrappedRedis) AtomicGet(ctx context.Context, key string) (rconfig, error) {
	return rconfig{}, nil
}

func (p *WrappedRedis) AtomicSet(ctx context.Context, key string, value string) (rconfig, error) {
	return rconfig{}, nil
}

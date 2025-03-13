package iredis

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/event"
)

type WrappedRedis struct {
	prefix string

	_getConn func() *redis.Client
}

func NewIredis(fn func() *redis.Client, prefix ...string) *WrappedRedis {
	if len(prefix) == 0 || prefix[0] == "" {
		return &WrappedRedis{_getConn: fn, prefix: "rrcc"}
	}
	return &WrappedRedis{_getConn: fn, prefix: prefix[0]}
}

func (p *WrappedRedis) AtomicGet(ctx context.Context, key string) (event.Event, error) {
	conn := p._getConn()
	cmds, err := conn.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Get(ctx, p.keyData(key))
		pipe.Get(ctx, p.keyVersion(key))
		return nil
	})
	if err != nil {
		return event.MkEventChg("", "", 0), err
	}
	var val string
	if cmds[0].Err() == nil {
		val = cmds[0].(*redis.StringCmd).Val()
	} else if cmds[0].Err() == redis.Nil {
		return event.MkEventDel("", 0), cmds[0].Err()
	}
	var ver uint64
	if cmds[1].Err() == nil {
		verStr := cmds[1].(*redis.StringCmd).Val()
		ver, err = strconv.ParseUint(verStr, 10, 64)
		if err != nil {
			return event.MkEventChg("", "", 0), err
		}
	} else if cmds[1].Err() == redis.Nil {
		return event.MkEventDel("", 0), cmds[1].Err()
	} else {
		return event.MkEventChg("", "", 0), cmds[1].Err()
	}
	return event.MkEventChg(val, val, ver), nil
}

func (p *WrappedRedis) AtomicSet(ctx context.Context, key string, value string) (event.Event, error) {
	conn := p._getConn()
	cmds, err := conn.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.GetSet(ctx, p.keyData(key), value)
		pipe.Incr(ctx, p.keyVersion(key))
		return nil
	})
	if err != nil {
		return event.MkEventChg("", "", 0), err
	}

	// If the original value is nil, use event ADD
	// TODO: impl
	getSetCmd := cmds[0].(*redis.StringCmd)
	if getSetCmd.Err() == redis.Nil {
		return event.MkEventChg("", "", 0), getSetCmd.Err()
	}

	incrCmd := cmds[1].(*redis.IntCmd)
	newVer, err := incrCmd.Result()
	if err != nil {
		return event.MkEventChg("", "", 0), err
	}
	return event.MkEventChg(value, value, uint64(newVer)), nil
}

func (p *WrappedRedis) BlockOnSetNX(ctx context.Context, key string) bool {
	conn := p._getConn()
	expiration := 5 * time.Second
	retryInterval := 100 * time.Millisecond // Retry every 100ms

	for {
		select {
		case <-ctx.Done():
			return false
		default:
			cmd := conn.SetNX(ctx, p.keyLock(key), "locked", expiration)
			if cmd.Err() != nil {
				time.Sleep(retryInterval)
				continue
			}
			if cmd.Val() {
				// Lock acquired successfully
				return true
			}
			time.Sleep(retryInterval)
		}
	}
}

package iredis

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/event"
)

type WrappedRedis struct {
	GetConn func() *redis.Client
}

func (p *WrappedRedis) AtomicGet(ctx context.Context, key string) (event.Event, error) {
	conn := p.GetConn()
	cmds, err := conn.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Get(ctx, key)
		pipe.Get(ctx, key+":ver")
		return nil
	})
	if err != nil {
		return event.MkEventChg("", "", 0), err
	}
	var val string
	if cmds[0].Err() == nil {
		val = cmds[0].(*redis.StringCmd).Val()
	} else if cmds[0].Err() != redis.Nil {
		return event.MkEventChg("", "", 0), cmds[0].Err()
	}
	var ver uint64
	if cmds[1].Err() == nil {
		verStr := cmds[1].(*redis.StringCmd).Val()
		ver, err = strconv.ParseUint(verStr, 10, 64)
		if err != nil {
			return event.MkEventChg("", "", 0), err
		}
	} else if cmds[1].Err() != redis.Nil {
		return event.MkEventChg("", "", 0), cmds[1].Err()
	}
	return event.MkEventChg(val, val, ver), nil
}

func (p *WrappedRedis) AtomicSet(ctx context.Context, key string, value string) (event.Event, error) {
	conn := p.GetConn()
	cmds, err := conn.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, key, value, 0)
		pipe.Incr(ctx, key+":ver")
		return nil
	})
	if err != nil {
		return event.MkEventChg("", "", 0), err
	}
	incrCmd := cmds[1].(*redis.IntCmd)
	newVer, err := incrCmd.Result()
	if err != nil {
		return event.MkEventChg("", "", 0), err
	}
	return event.MkEventChg(value, value, uint64(newVer)), nil
}

func (p *WrappedRedis) BlockOnSetNX(ctx context.Context, key string) bool {
	key = key + ":lock"
	conn := p.GetConn()
	expiration := 5 * time.Second
	retryInterval := 100 * time.Millisecond // Retry every 100ms

	for {
		select {
		case <-ctx.Done():
			return false
		default:
			cmd := conn.SetNX(ctx, key, "locked", expiration)
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

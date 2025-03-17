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

func (p *WrappedRedis) Get(ctx context.Context, key string) (event.Event, error) {
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
	var ver int64
	if cmds[1].Err() == nil {
		verStr := cmds[1].(*redis.StringCmd).Val()
		ver, err = strconv.ParseInt(verStr, 10, 64)
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

func (p *WrappedRedis) Set(ctx context.Context, key string, value string, ver int64) (event.Event, error) {
	conn := p._getConn()
	cmds, err := conn.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.GetSet(ctx, p.keyData(key), value)
		if ver > 0 {
			pipe.Set(ctx, p.keyVersion(key), ver, 0)
		} else {
			pipe.Incr(ctx, p.keyVersion(key))
		}
		return nil
	})

	// If the original value is nil, use event ADD
	getSetCmd := cmds[0].(*redis.StringCmd)
	if getSetCmd.Err() == redis.Nil {
		return event.MkEventAdd("", 0), nil
	} else if getSetCmd.Err() != nil {
		return event.MkEventChg("", "", 0), err
	}

	if ver > 0 {
		return event.MkEventChg(value, value, ver), nil
	}
	incrCmd := cmds[1].(*redis.IntCmd)
	newVer, err := incrCmd.Result()
	if err != nil {
		return event.MkEventChg("", "", 0), err
	}
	return event.MkEventChg(value, value, newVer), nil
}

func (p *WrappedRedis) BlockSetNX(ctx context.Context, key, value string) bool {
	conn := p._getConn()
	expiration := 5 * time.Second
	retryInterval := 100 * time.Millisecond // Retry every 100ms

	for {
		select {
		case <-ctx.Done():
			return false
		default:
			cmd := conn.SetNX(ctx, p.keyLock(key), value, expiration)
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

// Unlock deletes the lock key only if its value matches the provided value
func (p *WrappedRedis) MatchDelNX(ctx context.Context, key, value string) error {
	script := redis.NewScript(`
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
    `)
	_, err := script.Run(ctx, p._getConn(), []string{p.keyLock(key)}, value).Result()
	return err
}

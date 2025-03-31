package iredis

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/event"
)

type WrappedRedis struct {
	prefix string

	getConn func() *redis.Client
}

func NewIredis(fn func() *redis.Client, prefix ...string) *WrappedRedis {
	if len(prefix) == 0 || prefix[0] == "" {
		return &WrappedRedis{getConn: fn, prefix: "rrcc"}
	}
	return &WrappedRedis{getConn: fn, prefix: prefix[0]}
}

func (p *WrappedRedis) Get(ctx context.Context, key string) (event.Event, error) {
	conn := p.getConn()
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

func (p *WrappedRedis) Set(ctx context.Context, key string, value string) (event.Event, error) {
	conn := p.getConn()
	cmds, err := conn.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.GetSet(ctx, p.keyData(key), value)
		pipe.Incr(ctx, p.keyVersion(key))
		return nil
	})

	// If the original value is nil, use event ADD
	getSetCmd := cmds[0].(*redis.StringCmd)
	if getSetCmd.Err() != nil && getSetCmd.Err() != redis.Nil {
		return event.MkEventChg("", "", 0), err
	}

	incrCmd := cmds[1].(*redis.IntCmd)
	newVer, err := incrCmd.Result()
	if err != nil && err != redis.Nil {
		return event.MkEventChg("", "", 0), err
	}

	return event.MkEventChg(value, value, newVer), nil
}

// Update Version od Event with lua script, if Key not exist, just set it. If the current value
// equals the new value, do nothing, if current Version is greater than the new value return a error.
// Otherwise set the new value.
func (p *WrappedRedis) SetVer(ctx context.Context, key string, ver int64) error {
	redisClient := p.getConn()
	script := `
        local current_ver = redis.call('GET', KEYS[1])
        if not current_ver then
            redis.call('SET', KEYS[1], ARGV[1])
            return 0
        end
        local current_num = tonumber(current_ver)
        if not current_num then
            return {err="current version is not a number"}
        end
        local new_num = tonumber(ARGV[1])
        if not new_num then
            return {err="new version is not a number"}
        end
        if current_num == new_num then
            return 0
        elseif current_num > new_num then
            return {err="current version is greater than new version"}
        else
            redis.call('SET', KEYS[1], ARGV[1])
            return 0
        end
    `
	_, err := redisClient.Eval(ctx, script, []string{key}, ver).Result()
	return err
}

func (p *WrappedRedis) BlockSetNX(ctx context.Context, key string, due time.Time) (string, bool) {
	conn := p.getConn()
	value := uuid.New().String()
	retryInterval := 100 * time.Millisecond // Retry every 100ms

	for {
		select {
		case <-ctx.Done():
			return "", false
		default:
			expiration := time.Until(due)
			if expiration <= 0 {
				return "", false
			}
			cmd := conn.SetNX(ctx, p.keyLock(key), value, expiration)
			if cmd.Err() != nil {
				time.Sleep(retryInterval)
				continue
			}
			if cmd.Val() {
				return value, true
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
	_, err := script.Run(ctx, p.getConn(), []string{p.keyLock(key)}, value).Result()
	return err
}

package rrcc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type optionPoll func(*optionsPoll)

type optionsPoll struct {
	create       *string
	interval     int
	retry        int
	jitter       func(interval int) int
	redisTimeout time.Duration
	onClose      func(poller, error)
}

var defaultOptionsPoll = optionsPoll{
	interval:     10,
	retry:        3,
	jitter:       func(interval int) int { return interval },
	redisTimeout: 5 * time.Second,
	onClose: func(p poller, err error) {
		slog.Error(fmt.Sprintf("poller on key %s closed", p.Key()), "err", err)
	},
}

type entryPoller struct {
	key             string
	val             string
	fail            int
	options         optionsPoll
	connGetter      func() *redis.Client
	onCloseCallback func(poller, error)
}

func (p *entryPoller) Key() string {
	return p.key
}

func (p *entryPoller) Watch(cb func(event)) {
	ch := p.watchUpdate()
	go func() {
		for {
			select {
			case event, ok := <-ch:
				if ok {
					cb(event)
				} else {
					slog.Error("channel closed")
					return
				}
			}
		}
	}()
}

func (p *entryPoller) watchUpdate() <-chan event {
	ch := make(chan event)
	go func() {
		var err error
		var val string

	loop:
		for {
			conn := p.connGetter()
			ctx, _ := context.WithTimeout(context.Background(), p.options.redisTimeout)
			if conn == nil {
				err = errors.New("nil redis connection")
				goto fail
			}

			if t := p.options.jitter(p.options.interval); t <= 0 {
				err = fmt.Errorf("invalid interval sleep time %d", t)
				break loop
			} else {
				time.Sleep(time.Duration(t) * time.Second)
			}

			val, err = conn.Get(ctx, p.key).Result()
			if err != nil {
				err = fmt.Errorf("failed to get key %s: %w", p.key, err)
				goto fail
			}

			if p.val == "" && p.options.create != nil {
				ch <- mkEventAdd(val)
			} else if val != p.val {
				ch <- mkEventModify(p.val, val)
			}
			p.val = val
			continue

		fail:
			if p.fail < p.options.retry {
				p.fail += 1
			} else {
				break loop
			}
		}
		close(ch)
		p.onCloseCallback(p, err)
	}()
	return ch
}

func (p *entryPoller) Update(updateFn func() string) error {
	// set nx
	conn := p.connGetter()
	ctx, _ := context.WithTimeout(context.Background(), p.options.redisTimeout)

	for iflock := false; iflock == false; {
		res, err := conn.SetNX(ctx, p.key, "", 0).Result()
		if err != nil {
			return fmt.Errorf("failed to set key %s: %w", p.key, err)
		}
		iflock = res
		time.Sleep(100 * time.Millisecond)
	}

	// if set successfully, double check the val in redis
	val, err := conn.Get(ctx, p.key).Result()
	if err != nil {
		// p.onCloseCallback(p, err)
	}

	if val != p.val {
		p.val = val
	} else {
		// if val is the same as the old one, then emit the update
		p.val = updateFn()
		conn.Set(ctx, p.key, p.val, 0)
	}

	// unset nx lock
	conn.Del(ctx, p.key)

	return nil
}

func WithDefaultValue(value string) optionPoll {
	return func(opts *optionsPoll) {
		if value == "" {
			return
		}
		opts.create = &value
	}
}

func WithInterval(interval int) optionPoll {
	return func(opts *optionsPoll) {
		opts.interval = interval
	}
}

func WithRetry(retry int) optionPoll {
	return func(opts *optionsPoll) {
		opts.retry = retry
	}
}

func WithJitter(jitter func(interval int) int) optionPoll {
	return func(opts *optionsPoll) {
		opts.jitter = jitter
	}
}

func WithRedisTimeout(timeout time.Duration) optionPoll {
	return func(opts *optionsPoll) {
		opts.redisTimeout = timeout
	}
}

func WithOnClose(onClose func(poller, error)) optionPoll {
	return func(opts *optionsPoll) {
		opts.onClose = onClose
	}
}

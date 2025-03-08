package rrcc

import (
	"context"
	"time"

	"github.com/humbornjo/rrcc/internal/btree"
	"github.com/humbornjo/rrcc/internal/iredis"
	"github.com/redis/go-redis/v9"
)

type client struct {
	mcache btree.Btree[string, []event]

	_ctx     context.Context
	_cancel  func()
	_iclient iredis.WrappedRedis
}

func initClient(ctx context.Context, fn func() *redis.Client) *client {
	cctx, cancel := context.WithCancel(ctx)
	client := &client{
		_ctx:     cctx,
		_cancel:  cancel,
		_iclient: iredis.WrappedRedis{GetConn: fn},
	}
	client.watch(cctx)
	return client
}

func (p *client) String(k string, opts ...optionPoll) poller {
	options := defaultOptionsPoll
	for _, opt := range opts {
		opt(&options)
	}

	p.mcache.Put(k, func(old []event, exists bool) (new []event, write bool) {
		if exists {
			return old, false
		}
		return nil, true
	})

	cctx, cancel := context.WithCancel(p._ctx)
	return &basePoller{
		_ctx: cctx,
		_cancel: func() {
			cancel()
			p.mcache.Delete(k)
		},
		_update: p.update,
		key:     k,
		options: options,
	}
}

func (p *client) Stop() {

}

// TODO: impl
func (p *client) Bind(k string, s any, opts ...optionPoll) poller {
	return p.String(k, opts...)
}

func (p *client) watch(ctx context.Context) {
	// iter over all the keys in the cache and watch them once a while
	go func() {
	}()
}

func (p *client) update(key string, oldValue event, upd Updater) (event, error) {
	// get events from cache first, if version larger than the old one, return
	// TODO: impl

	// query the value from redis, incase the internal cache is not up to date
	newValue, err := p._iclient.AtomicGet(p._ctx, key)
	if err != nil {
		return oldValue, err
	}

	if newValue.Version > oldValue.version {
		return oldValue.mergeEvents([]event{
			event{NewValue: newValue.Value, version: newValue.Version},
		})
	}

	// the value in redis is not up to date, set a lock to update it
	// TODO: refactor
	for {
		_, err := conn.SetNX(p._ctx, key, "", 0).Result()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer conn.Del(p._ctx, key)

	// if set successfully, double check the val in redis to make sure the version is not updated
	newValue, err = p._iclient.AtomicGet(p._ctx, key)
	if err != nil {
		return oldValue, err
	}

	if newValue.Version > oldValue.version {
		return oldValue.mergeEvents([]event{
			event{NewValue: newValue.Value, version: newValue.Version},
		})
	}

	// generate the new value and set it into redis
	newValue, err = p._iclient.AtomicSet(p._ctx, key, upd())
	if err != nil {
		return oldValue, err
	}

	return oldValue.mergeEvents([]event{
		event{NewValue: newValue.Value, version: newValue.Version},
	})
}

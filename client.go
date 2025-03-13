package rrcc

import (
	"context"
	"time"
	"unsafe"
	"weak"

	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/btree"
	"github.com/humbornjo/rrcc/internal/iredis"
)

type eventWatch struct {
	key string

	_addr weak.Pointer[[]Event]
}

type client struct {
	mcache btree.Btree[string, weak.Pointer[[]Event]]

	_ch      chan eventWatch
	_ctx     context.Context
	_cancel  func()
	_iclient iredis.WrappedRedis
}

func clientInit(ctx context.Context, fn func() *redis.Client) (*client, error) {
	cctx, cancel := context.WithCancel(ctx)
	client := &client{
		mcache: btree.NewBtree[string, weak.Pointer[[]Event]](btree.CmpString),

		_ch:      make(chan eventWatch, 32),
		_ctx:     cctx,
		_cancel:  cancel,
		_iclient: *iredis.NewIredis(fn),
	}

	if err := fn().Ping(ctx).Err(); err != nil {
		return nil, err
	}

	client.watch(cctx)
	return client, nil
}

func (p *client) Data(key string, opts ...optionPoll) poller {
	options := defaultOptionsPoll
	for _, opt := range opts {
		opt(&options)
	}

	var ptr unsafe.Pointer
	p.mcache.AtomicPut(
		key,
		func(old weak.Pointer[[]Event], exists bool) (weak.Pointer[[]Event], bool) {
			if new := old.Value(); exists && new != nil {
				ptr = unsafe.Pointer(new)
				return old, false
			}
			es := make([]Event, 0)
			ptr = unsafe.Pointer(&es)
			p._ch <- eventWatch{key: key, _addr: weak.Make(&es)}
			return weak.Make(&es), true
		})

	cctx, cancel := context.WithCancel(p._ctx)
	return &basePoller{
		key:     key,
		options: options,

		_addr:   ptr,
		_ctx:    cctx,
		_cancel: func() { cancel() },
		_update: p.update,
	}
}

func (p *client) Stop() {
	p._cancel()
}

// TODO: impl
func (p *client) Bind(k string, s any, opts ...optionPoll) poller {
	return p.Data(k, opts...)
}

func (p *client) watch(ctx context.Context) {
	ews := make([]eventWatch, 0)
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-p._ctx.Done():
				p._cancel()
				return
			case ew := <-p._ch:
				ews = append(ews, ew)
			case <-ticker.C:
				ewsNew := make([]eventWatch, 0, len(ews))
				for _, ew := range ews {
					if pes := ew._addr.Value(); pes != nil {
						ewsNew = append(ewsNew, ew)
						go p.updateCache(ctx, ew, time.Now())
					}
				}
				ews = ewsNew
			}
		}
	}()
}

func (p *client) update(key string, oldValue Event, upd Updater) (Event, error) {
	// Try get events from cache first, if event version in cache is greater than the old one, return
	if wk, ok := p.mcache.AtomicGet(key); ok {
		if pes := wk.Value(); pes != nil {
			if es := *pes; len(es) > 0 {
				return oldValue.MergeEvents(es)
			}
		}
	}

	tctx, cancel := context.WithTimeout(p._ctx, 5*time.Second)
	defer cancel()

	// If the evnets in cache are not as up-to-date as the given one, search redis for the newest one
	if newValue, err := p._iclient.AtomicGet(tctx, key); err == nil &&
		newValue.Version > oldValue.Version {
		return oldValue.MergeEvents([]Event{newValue})
	}

	// If no updator is provided, then return, omit the updating phase
	if upd == nil {
		return oldValue, nil
	}

	// Recently event from redis is still not as up to date as the given one, try setNX, ready to update
	if !p._iclient.BlockOnSetNX(tctx, key) {
		return oldValue, ErrRedisSetNX
	}

	// Set successfully, double check the event in redis to make sure it really need to be updated
	if newValue, err := p._iclient.AtomicGet(tctx, key); err == nil &&
		newValue.Version > oldValue.Version {
		return oldValue.MergeEvents([]Event{newValue})
	}

	// Generate the new value, making event and set it redis, INCR the version
	if newValue, err := p._iclient.AtomicSet(tctx, key, upd()); err != nil {
		return oldValue, err
	} else {
		return oldValue.MergeEvents([]Event{newValue})
	}
}

// Event is updated every 10 seconds by default, if the order needs perfectly ensured, then we still need
// a timestamp to record the sequence on client side. And, this timestamp should better be aligned among
// all clients. Thus the perfect choice is nanosecond timestamp.
func (p *client) updateCache(ctx context.Context, ew eventWatch, _ time.Time) {
	e, err := p._iclient.AtomicGet(ctx, ew.key)
	if err != nil {
		return
	}
	p.mcache.AtomicPut(
		ew.key,
		func(oldV weak.Pointer[[]Event], exists bool) (weak.Pointer[[]Event], bool) {
			if !exists {
				panic("unreachable")
			}
			if pes := oldV.Value(); pes == nil {
				panic("unreachable")
			} else {
				*pes = append(*pes, e)
				return oldV, false
			}
		})
}

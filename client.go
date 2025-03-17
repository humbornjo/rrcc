package rrcc

import (
	"context"
	"sync"
	"time"
	"unsafe"
	"weak"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/btree"
	"github.com/humbornjo/rrcc/internal/event"
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

		_mu:     sync.RWMutex{},
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
						go p.updateCache(ctx, ew)
					}
				}
				ews = ewsNew
			}
		}
	}()
}

func (p *client) update(key string, oldEvent Event, upd Updater) (Event, error) {
	tctx, cancel := context.WithTimeout(p._ctx, 3*time.Second)
	defer cancel()

	var ver int64
	if newEvent, err := p._update(tctx, key, oldEvent); upd == nil {
		if err != ErrRemoteOutOfDate {
			return newEvent, err
		}
		ver = oldEvent.Version
	} else {
		if err == nil {
			return newEvent, nil
		}
	}

	// Recently event from redis is still not as up to date as the given one, try setNX, ready to update
	if lockValue := uuid.New().String(); !p._iclient.BlockSetNX(tctx, key, lockValue) {
		return oldEvent, ErrRedisSetNX
	} else {
		defer p._iclient.MatchDelNX(tctx, key, lockValue)
	}

	// Set successfully, double check the event in redis to make sure it really need to be updated
	if newEvent, err := p._iclient.Get(tctx, key); err == nil {
		if newEvent.Version > oldEvent.Version {
			return oldEvent.MergeEvents([]Event{newEvent})
		} else if newEvent.Version == oldEvent.Version {
			if upd == nil {
				return oldEvent.MergeEvents([]Event{newEvent})
			}
		} else {
			upd = func() string { return oldEvent.NewValue }
		}
	}

	// Generate the new value, making event and set it redis, INCR the version
	newEvent, err := p._iclient.Set(tctx, key, upd(), ver)
	if err != nil {
		return oldEvent, err
	}

	return oldEvent.MergeEvents([]Event{newEvent})
}

func (p *client) _update(ctx context.Context, key string, oldEvent Event) (Event, error) {
	// Try get events from cache first, if event version in cache is greater than the old one, return
	if wk, ok := p.mcache.AtomicGet(key); ok {
		p.mcache.RLock()
		if pes := wk.Value(); pes != nil && len(*pes) > 0 {
			es := *pes
			if es[len(es)-1].Version > oldEvent.Version {
				p.mcache.RUnlock()
				return oldEvent.MergeEvents(es)
			}
		}
		p.mcache.RUnlock()
	}

	// If the evnets in cache are not as up-to-date as the given one, search redis for the newest one
	if newEvent, err := p._iclient.Get(ctx, key); err == nil {
		if newEvent.Version < oldEvent.Version {
			return oldEvent, ErrRemoteOutOfDate
		} else {
			return oldEvent.MergeEvents([]Event{newEvent})
		}
	}

	return oldEvent, event.ErrUnchanged
}

func (p *client) updateCache(ctx context.Context, ew eventWatch) {
	e, err := p._iclient.Get(ctx, ew.key)
	if err != nil || e.Version < 1 {
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
				if len(*pes) > 64 {
					*pes = (*pes)[len(*pes)-64:]
				}
				return oldV, false
			}
		})
}

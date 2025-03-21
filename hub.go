package rrcc

import (
	"context"
	"log/slog"
	"time"
	"unsafe"
	"weak"

	"github.com/humbornjo/vino"
	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/btree"
	"github.com/humbornjo/rrcc/internal/iredis"
)

type pollerHandler struct {
	key       string
	_addr     weak.Pointer[[]Event]
	_signalFn func(*Event) bool
}

type hubEntry struct {
	lrEvent   Event
	_wakerCh  chan struct{}
	_handlers []pollerHandler
}

type hub struct {
	hcache     map[string]*hubEntry
	mcache     btree.Btree[string, weak.Pointer[[]Event]]
	_mu        vino.RWMutex
	_ctx       context.Context
	_cancel    context.CancelFunc
	_iclient   iredis.WrappedRedis
	_handlerCh chan pollerHandler
}

func initHub(ctx context.Context, fn func() *redis.Client) (*hub, error) {
	cctx, cancel := context.WithCancel(ctx)
	client := &hub{
		hcache:     make(map[string]*hubEntry),
		mcache:     btree.NewBtree[string, weak.Pointer[[]Event]](btree.CmpString),
		_ctx:       cctx,
		_cancel:    cancel,
		_iclient:   *iredis.NewIredis(fn),
		_handlerCh: make(chan pollerHandler, 32),
	}

	if err := fn().Ping(ctx).Err(); err != nil {
		return nil, err
	}

	go client.start()
	return client, nil
}

func (p *hub) Data(key string, opts ...PollOption) poller {
	config := defaultPollConfig
	for _, opt := range opts {
		opt(&config)
	}

	var ptr unsafe.Pointer
	updateCh := make(chan Event, 1)
	cctx, cancel := context.WithCancel(p._ctx)
	poller := &basePoller{
		key:               key,
		keepAlive:         config.keepAlive,
		recvHeartbeat:     config.recvHeartbeat,
		_addr:             ptr,
		_ctx:              cctx,
		_cancel:           cancel,
		_updateCh:         updateCh,
		_onWatchCloseHook: config._onWatchCloseHook,
	}

	p.mcache.AtomicPut(
		key,
		func(old weak.Pointer[[]Event], exists bool) (weak.Pointer[[]Event], bool) {
			if new := old.Value(); exists && new != nil {
				ptr = unsafe.Pointer(new)
				return old, false
			}
			es := make([]Event, 0)
			ptr = unsafe.Pointer(&es)
			p._handlerCh <- pollerHandler{key, weak.Make(&es), signalfunc(poller)}
			return weak.Make(&es), true
		})

	return poller
}

func (p *hub) Stop() {
	p._cancel()
}

// TODO: impl
func (p *hub) Bind(k string, s any, opts ...PollOption) poller {
	return p.Data(k, opts...)
}

func (p *hub) start() {
	for {
		select {
		case <-p._ctx.Done():
			p._cancel()
			return
		case handler := <-p._handlerCh:
			p._mu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			if _, ok := p.hcache[handler.key]; !ok {
				waker := make(chan struct{})
				p.hcache[handler.key] = &hubEntry{_wakerCh: waker}
				go p.startKey(handler.key, waker, ctx.Done())
			}
			handlers := p.hcache[handler.key]._handlers
			p.hcache[handler.key]._handlers = append(handlers, handler)
			cancel()
			p._mu.Unlock()
		}
	}
}

func (p *hub) startKey(key string, waker <-chan struct{}, blocker <-chan struct{}) {
	<-blocker
	// TODO: make ticker interval configurable
	ticker := time.NewTicker(10 * time.Second)
	for {
		var e *Event
		select {
		case <-waker:
			p._mu.RLock()
			dup := p.hcache[key].lrEvent
			e = &dup
			slog.Debug("Sig from waker", "key", key, "event", e)
			p._mu.RUnlock()
		case <-ticker.C:
			e = p.updateCache(p._ctx, key, nil)
		}

		p._mu.RLock()
		if len(p.hcache[key]._handlers) == 0 {
			p._mu.RUnlock()
			return
		}

		if e != nil {
			p.mcache.AtomicPut(key, func(old weak.Pointer[[]Event], exists bool) (weak.Pointer[[]Event], bool) {
				if !exists {
					return old, false
				}
				if xs := old.Value(); xs != nil {
					if len(*xs) > 0 {
						last := (*xs)[len(*xs)-1]
						if last.Version == e.Version {
							e.Type = PING
							return old, true
						}
					}
					*xs = append(*xs, *e)
					return old, true
				}
				return old, false
			})
		}

		handlers := []pollerHandler{}
		for _, h := range p.hcache[key]._handlers {
			if h._signalFn(e) {
				handlers = append(handlers, h)
			}
		}
		p._mu.RUpgrade()
		p.hcache[key]._handlers = handlers
		p._mu.Unlock()
	}
}

func (p *hub) Update(key string, upd Updater, expiration time.Duration) error {
	ctx, cancel := context.WithTimeout(p._ctx, 5*time.Second)
	defer cancel()
	if e := p.updateCache(ctx, key, nil); e != nil {
		// Remote already has newer event
		slog.Debug("Remote already has newer event", "key", key, "local", e, "remote", *e)
		return nil
	}

	due, _ := ctx.Deadline()
	if val, ok := p._iclient.BlockSetNX(ctx, key, due); !ok {
		return ErrRedisSetNX
	} else {
		defer p._iclient.MatchDelNX(ctx, key, val)
	}

	// Double check
	if e := p.updateCache(ctx, key, nil); e != nil {
		slog.Debug("Remote already has newer event", "key", key, "local", e, "remote", *e)
		return nil
	}

	val, err := upd()
	if err != nil {
		slog.Debug("Failed to update local event", "key", key, "error", err)
		return err
	}

	// Update local event
	if e, err := p._iclient.Set(ctx, key, val); err != nil {
		slog.Debug("Failed to update local event", "key", key, "error", err)
		return err
	} else {
		p._mu.RLock()
		lrEvent := p.hcache[key].lrEvent
		p._mu.RUnlock()
		newEvent := lrEvent.MergeEvents([]Event{e})
		p.updateCache(ctx, key, &newEvent)
		slog.Debug("Updated local event", "key", key, "event", newEvent)
		return nil
	}
}

// Update cache event for key, if the remote event is newer than the latest local
// event return the event. Otherwise return nil. If remote event's version is smaller
// OR same version but different value, emit a routine to update the remote version
// to [LOCAL_VERSIOn + 1] via lua script.
func (p *hub) updateCache(ctx context.Context, key string, event *Event) *Event {
	hasUpdate := false
	defer func() {
		if hasUpdate {
			p._mu.RLock()
			if handler, ok := p.hcache[key]; ok {
				slog.Debug("Wake up handler", "key", key, "event", event)
				select {
				case handler._wakerCh <- struct{}{}:
				default:
				}
			}
			p._mu.RUnlock()
		}
	}()

	if event != nil {
		p._mu.Lock()
		lrEvent := p.hcache[key].lrEvent
		if lrEvent.Version < event.Version ||
			(lrEvent.Version == event.Version && lrEvent.NewValue != event.NewValue) {
			hasUpdate = true
			p.hcache[key].lrEvent = *event
			p._mu.Unlock()
			return event
		}
		p._mu.Unlock()
		return nil
	}

	newEvent, err := p._iclient.Get(ctx, key)
	if err != nil {
		return nil
	}

	p._mu.RLock()
	oldEvent := p.hcache[key].lrEvent
	if oldEvent.Version > newEvent.Version {
		ver := oldEvent.Version + 1
		p._mu.RUnlock()
		if err := p._iclient.SetVer(ctx, key, ver); err != nil {
			return nil
		}
		newEvent.Version = ver
		p._mu.Lock()
		oldEvent = p.hcache[key].lrEvent
		if oldEvent.Version < newEvent.Version ||
			oldEvent.Version == newEvent.Version && oldEvent.NewValue != newEvent.NewValue {
			hasUpdate = true
			p.hcache[key].lrEvent = newEvent
			p._mu.Unlock()
			return &newEvent
		}
		p._mu.Unlock()
	}

	if oldEvent.Version < newEvent.Version ||
		oldEvent.Version == newEvent.Version && oldEvent.NewValue != newEvent.NewValue {
		p._mu.RUpgrade()
		hasUpdate = true
		p.hcache[key].lrEvent = newEvent
		p._mu.Unlock()
		return &newEvent
	}
	defer p._mu.RUnlock()
	p.mcache.AtomicPut(
		key,
		func(oldV weak.Pointer[[]Event], exists bool) (weak.Pointer[[]Event], bool) {
			if !exists {
				panic("unreachable")
			}
			if pes := oldV.Value(); pes == nil {
				panic("unreachable")
			} else {
				*pes = append(*pes, newEvent)
				if len(*pes) > 64 {
					*pes = (*pes)[len(*pes)-64:]
				}
				return oldV, false
			}
		})

	return nil
}

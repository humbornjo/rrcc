package rrcc

import (
	"context"
	"log/slog"
	"time"
	"unsafe"
	"weak"

	. "github.com/humbornjo/vino"
	"github.com/redis/go-redis/v9"

	"github.com/humbornjo/rrcc/internal/btree"
	"github.com/humbornjo/rrcc/internal/iredis"
)

type pollerHandler struct {
	key      string
	addrs    weak.Pointer[[]Event]
	signalFn func(*Event) bool
}

type hubEntry struct {
	lrEvent  Event
	wakerCh  chan struct{}
	handlers []pollerHandler
}

type hub struct {
	hcache map[string]*hubEntry
	mcache btree.Btree[string, weak.Pointer[[]Event]]

	mu        MutexRW
	ctx       context.Context
	cancel    context.CancelFunc
	iclient   iredis.WrappedRedis
	handlerCh chan pollerHandler
}

func initHub(ctx context.Context, fn func() *redis.Client) (*hub, error) {
	cctx, cancel := context.WithCancel(ctx)
	client := &hub{
		hcache: make(map[string]*hubEntry),
		mcache: btree.NewBtree[string, weak.Pointer[[]Event]](btree.CmpString),

		ctx:       cctx,
		cancel:    cancel,
		iclient:   *iredis.NewIredis(fn),
		handlerCh: make(chan pollerHandler, 32),
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
	cctx, cancel := context.WithCancel(p.ctx)
	poller := &basePoller{
		key:           key,
		keepAlive:     config.keepAlive,
		recvHeartbeat: config.recvHeartbeat,

		addr:             ptr,
		ctx:              cctx,
		cancel:           cancel,
		updateCh:         updateCh,
		onWatchCloseHook: config._onWatchCloseHook,
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
			p.handlerCh <- pollerHandler{key, weak.Make(&es), signalFunc(poller)}
			return weak.Make(&es), true
		})

	return poller
}

func (p *hub) Stop() {
	p.cancel()
}

// TODO: impl
func (p *hub) Bind(k string, s any, opts ...PollOption) poller {
	return p.Data(k, opts...)
}

func (p *hub) start() {
	for {
		select {
		case <-p.ctx.Done():
			p.cancel()
			return
		case handler := <-p.handlerCh:
			p.mu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			if _, ok := p.hcache[handler.key]; !ok {
				waker := make(chan struct{})
				p.hcache[handler.key] = &hubEntry{wakerCh: waker}
				go p.startKey(handler.key, waker, ctx.Done())
			}
			handlers := p.hcache[handler.key].handlers
			p.hcache[handler.key].handlers = append(handlers, handler)
			cancel()
			p.mu.Unlock()
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
			p.mu.RLock()
			dup := p.hcache[key].lrEvent
			e = &dup
			slog.Debug("Sig from waker", "key", key, "event", e)
			p.mu.RUnlock()
		case <-ticker.C:
			e = p.updateCache(p.ctx, key, nil)
		}

		p.mu.RLock()
		if len(p.hcache[key].handlers) == 0 {
			p.mu.RUnlock()
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
		for _, h := range p.hcache[key].handlers {
			if h.signalFn(e) {
				handlers = append(handlers, h)
			}
		}
		p.mu.RLifted()
		p.hcache[key].handlers = handlers
		p.mu.Unlock()
	}
}

func (p *hub) Update(key string, upd Updater, expiration time.Duration) error {
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	if e := p.updateCache(ctx, key, nil); e != nil {
		// Remote already has newer event
		slog.Debug("Remote already has newer event", "key", key, "local", e, "remote", *e)
		return nil
	}

	due, _ := ctx.Deadline()
	if val, ok := p.iclient.BlockSetNX(ctx, key, due); !ok {
		return ErrRedisSetNX
	} else {
		defer p.iclient.MatchDelNX(ctx, key, val)
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
	if e, err := p.iclient.Set(ctx, key, val); err != nil {
		slog.Debug("Failed to update local event", "key", key, "error", err)
		return err
	} else {
		p.mu.RLock()
		lrEvent := p.hcache[key].lrEvent
		p.mu.RUnlock()
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
			p.mu.RLock()
			if handler, ok := p.hcache[key]; ok {
				slog.Debug("Wake up handler", "key", key, "event", event)
				select {
				case handler.wakerCh <- struct{}{}:
				default:
				}
			}
			p.mu.RUnlock()
		}
	}()

	if event != nil {
		p.mu.Lock()
		lrEvent := p.hcache[key].lrEvent
		if lrEvent.Version < event.Version ||
			(lrEvent.Version == event.Version && lrEvent.NewValue != event.NewValue) {
			hasUpdate = true
			p.hcache[key].lrEvent = *event
			p.mu.Unlock()
			return event
		}
		p.mu.Unlock()
		return nil
	}

	newEvent, err := p.iclient.Get(ctx, key)
	if err != nil {
		return nil
	}

	p.mu.RLock()
	oldEvent := p.hcache[key].lrEvent
	if oldEvent.Version > newEvent.Version {
		ver := oldEvent.Version + 1
		p.mu.RUnlock()
		if err := p.iclient.SetVer(ctx, key, ver); err != nil {
			return nil
		}
		newEvent.Version = ver
		p.mu.Lock()
		oldEvent = p.hcache[key].lrEvent
		if oldEvent.Version < newEvent.Version ||
			oldEvent.Version == newEvent.Version && oldEvent.NewValue != newEvent.NewValue {
			hasUpdate = true
			p.hcache[key].lrEvent = newEvent
			p.mu.Unlock()
			return &newEvent
		}
		p.mu.Unlock()
	}

	if oldEvent.Version < newEvent.Version ||
		oldEvent.Version == newEvent.Version && oldEvent.NewValue != newEvent.NewValue {
		p.mu.RLifted()
		hasUpdate = true
		p.hcache[key].lrEvent = newEvent
		p.mu.Unlock()
		return &newEvent
	}
	defer p.mu.RUnlock()
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

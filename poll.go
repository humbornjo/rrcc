package rrcc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unsafe"

	"github.com/humbornjo/rrcc/internal/event"
)

type Updater = func() string

var defaultOptionsPoll = optionsPoll{
	interval:         5,
	enableEncryption: false,

	_create: nil,
	_jitter: func(interval int) int { return interval },
	_onFailHook: func(p poller, err error) {
		slog.Error(fmt.Sprintf("poller on key %s failed", p.Key()), "err", err)
	},
	_onCloseHook: func(p poller, err error) {
		slog.Error(fmt.Sprintf("poller on key %s closed", p.Key()), "err", err)
	},
}

type poller interface {
	Cancel()
	Key() string
	Watch(func(Event))
	Update(Updater) (Event, error)
}

type basePoller struct {
	key     string
	latest  Event
	options optionsPoll

	_mu     sync.RWMutex
	_addr   unsafe.Pointer
	_ctx    context.Context
	_cancel func()
	_update func(string, Event, Updater) (Event, error)
}

func (p *basePoller) Key() string {
	return p.key
}

func (p *basePoller) Watch(cb func(Event)) {
	ch := p.watchUpdate()
	go func() {
		for {
			select {
			case event, ok := <-ch:
				if ok {
					cb(event)
				} else {
					return
				}
			}
		}
	}()
}

func (p *basePoller) Cancel() {
	p._addr = nil
	p._cancel()
}

func (p *basePoller) Update(fn Updater) (Event, error) {
	p._mu.RLock()
	e := p.latest
	p._mu.RUnlock()
	return p._update(p.key, e, fn)
}

func (p *basePoller) poll() (Event, error) {
	p._mu.RLock()
	e := p.latest
	p._mu.RUnlock()
	return p._update(p.key, e, nil)
}

func (p *basePoller) watchUpdate() <-chan Event {
	ch := make(chan Event, 32)
	go func() {
		for {
			select {
			case <-p._ctx.Done():
				goto exit
			default:
				e, err := p.poll()
				p._mu.Lock()
				p.latest = e
				p._mu.Unlock()
				if err != nil {
					if !errors.Is(err, event.ErrUnchanged) {
						p.options._onFailHook(p, err)
					}
				} else {
					ch <- e
				}
				if t := p.options._jitter(p.options.interval); t <= 0 {
					p.options._onFailHook(p, ErrNegTime)
				} else {
					time.Sleep(time.Duration(t) * time.Second)
				}
			}
		}
	exit:
		close(ch)
		p.options._onCloseHook(p, ErrStopWatch)
	}()
	return ch
}

type structPoller struct {
	s any
	basePoller
}

type optionPoll func(*optionsPoll)

type optionsPoll struct {
	enableEncryption bool
	interval         int

	_create      *string
	_jitter      func(interval int) int
	_onFailHook  func(poller, error)
	_onCloseHook func(poller, error)
}

func WithDefaultValue(value string) optionPoll {
	return func(opts *optionsPoll) {
		if value == "" {
			return
		}
		opts._create = &value
	}
}

func WithInterval(interval int) optionPoll {
	return func(opts *optionsPoll) {
		opts.interval = interval
	}
}

func WithJitter(jitter func(interval int) int) optionPoll {
	return func(opts *optionsPoll) {
		opts._jitter = jitter
	}
}

func WithOnCloseHook(onClose func(poller, error)) optionPoll {
	return func(opts *optionsPoll) {
		opts._onCloseHook = onClose
	}
}

func WithOnFailHook(onFail func(poller, error)) optionPoll {
	return func(opts *optionsPoll) {
		opts._onFailHook = onFail
	}
}

func WithEncryption(enable bool) optionPoll {
	return func(opts *optionsPoll) {
		opts.enableEncryption = enable
	}
}

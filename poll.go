package rrcc

import (
	"context"
	"sync"
	"time"
	"unsafe"
)

type Updater = func() (string, error)

var defaultPollConfig = pollConfig{
	keepAlive:     1 * time.Hour,
	recvHeartbeat: false,

	_onWatchCloseHook: func(p poller) { p.Cancel() },
}

type poller interface {
	Key() string
	Watch(func(Event))
	Cancel()
}

type basePoller struct {
	key           string
	closed        bool
	keepAlive     time.Duration
	recvHeartbeat bool

	_mu               sync.Mutex
	_addr             unsafe.Pointer
	_ctx              context.Context
	_cancel           context.CancelFunc
	_updateCh         chan Event
	_onWatchCloseHook func(p poller)
}

func (p *basePoller) Key() string {
	return p.key
}

func (p *basePoller) Watch(callback func(Event)) {
	ch := p.watchUpdate()
	go func() {
		for {
			event, ok := <-ch
			if !ok {
				return
			}
			callback(event)
		}
	}()
}

func (p *basePoller) Cancel() {
	p._cancel()
	p._addr = nil
	p._mu.Lock()
	defer p._mu.Unlock()
	close(p._updateCh)
	p.closed = true
}

func (p *basePoller) poll() <-chan Event {
	return p._updateCh
}

func (p *basePoller) watchUpdate() <-chan Event {
	ch := make(chan Event, 32)
	go func() {
		defer p._onWatchCloseHook(p)
		for {
			select {
			case e := <-p.poll():
				if p.recvHeartbeat {
					ch <- e
					continue
				}
				if e.Type != PING {
					ch <- e
				}
			case <-p._ctx.Done():
				return
			case <-time.After(p.keepAlive):
				return
			}
		}
	}()
	return ch
}

type structPoller struct {
	s any
	basePoller
}

type PollOption func(*pollConfig)

type pollConfig struct {
	keepAlive     time.Duration
	recvHeartbeat bool

	_onWatchCloseHook func(poller)
}

func WithOnCloseHook(onClose func(poller)) PollOption {
	return func(config *pollConfig) {
		config._onWatchCloseHook = onClose
	}
}

func WithKeepalive(keepalive time.Duration) PollOption {
	return func(config *pollConfig) {
		config.keepAlive = keepalive
	}
}

func signalfunc(poller *basePoller) func(*Event) bool {
	return func(e *Event) bool {
		poller._mu.Lock()
		defer poller._mu.Unlock()
		if poller.closed {
			return false
		}

		if e == nil {
			return true
		}

		select {
		case poller._updateCh <- *e:
		default:
		}
		return true
	}
}

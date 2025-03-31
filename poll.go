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

	mu               sync.Mutex
	addr             unsafe.Pointer
	ctx              context.Context
	cancel           context.CancelFunc
	updateCh         chan Event
	onWatchCloseHook func(p poller)
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
	p.cancel()
	p.addr = nil
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.updateCh)
	p.closed = true
}

func (p *basePoller) poll() <-chan Event {
	return p.updateCh
}

func (p *basePoller) watchUpdate() <-chan Event {
	ch := make(chan Event, 32)
	go func() {
		defer p.onWatchCloseHook(p)
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
			case <-p.ctx.Done():
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

func signalFunc(poller *basePoller) func(*Event) bool {
	return func(e *Event) bool {
		poller.mu.Lock()
		defer poller.mu.Unlock()
		if poller.closed {
			return false
		}

		if e == nil {
			return true
		}

		select {
		case poller.updateCh <- *e:
		default:
		}
		return true
	}
}

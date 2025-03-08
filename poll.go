package rrcc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type Updater = func() string

var defaultOptionsPoll = optionsPoll{
	create:   nil,
	interval: 5,
	jitter:   func(interval int) int { return interval },
	onFailHook: func(p poller, err error) {
		slog.Error(fmt.Sprintf("poller on key %s failed", p.Key()), "err", err)
	},
	onCloseHook: func(p poller, err error) {
		slog.Error(fmt.Sprintf("poller on key %s closed", p.Key()), "err", err)
	},
}

type poller interface {
	Key() string
	Watch(func(event))
	Update(Updater) error

	Cancel()
}

type basePoller struct {
	_ctx    context.Context
	_cancel func()
	_update func(string, event, Updater) (event, error)

	key     string
	latest  event
	options optionsPoll
}

func (p *basePoller) Key() string {
	return p.key
}

func (p *basePoller) Watch(cb func(event)) {
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
	p._cancel()
}

func (p *basePoller) Update(updateFn Updater) error {
	_, err := p._update(p.key, p.latest, updateFn)
	return err
}

func (p *basePoller) poll() (event, error) {
	e := p.latest
	e.version = 0
	return p._update(p.key, e, nil)
}

func (p *basePoller) watchUpdate() <-chan event {
	ch := make(chan event, 32)
	go func() {
	loop:
		for {
			select {
			case <-p._ctx.Done():
				break loop
			default:
				e, err := p.poll()
				p.latest = e
				if err != nil && !errors.Is(err, errUnchanged) {
					p.options.onFailHook(p, err)
				} else {
					ch <- e
				}
				if t := p.options.jitter(p.options.interval); t <= 0 {
					p.options.onFailHook(p, ErrNegTime)
				} else {
					time.Sleep(time.Duration(t) * time.Second)
				}
			}
		}
		close(ch)
		p.options.onCloseHook(p, ErrStopWatch)
	}()
	return ch
}

type structPoller struct {
	s any
	basePoller
}

type optionPoll func(*optionsPoll)

type optionsPoll struct {
	create      *string
	interval    int
	jitter      func(interval int) int
	onFailHook  func(poller, error)
	onCloseHook func(poller, error)
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

func WithJitter(jitter func(interval int) int) optionPoll {
	return func(opts *optionsPoll) {
		opts.jitter = jitter
	}
}

func WithOnCloseHook(onClose func(poller, error)) optionPoll {
	return func(opts *optionsPoll) {
		opts.onCloseHook = onClose
	}
}

func WithOnFailHook(onFail func(poller, error)) optionPoll {
	return func(opts *optionsPoll) {
		opts.onFailHook = onFail
	}
}

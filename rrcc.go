package rrcc

import (
	"github.com/redis/go-redis/v9"
)

type poller interface {
	Key() string
	Watch(func(event))
	Update(func() string) error
}

type Client interface {
	Entry(k string, opts ...optionPoll) poller
	// Bind(k string, s any, opts ...optionPoll) poller
}

func NewClient(opts redis.Options, prefix string) Client {
	return &client{}
}

type client struct {
	entries []string
}

func (c *client) getConn() *redis.Client {
	return nil
}

func (p *client) Entry(k string, opts ...optionPoll) poller {
	options := defaultOptionsPoll
	for _, opt := range opts {
		opt(&options)
	}
	return &entryPoller{
		key:             k,
		options:         options,
		connGetter:      p.getConn,
		onCloseCallback: options.onClose,
	}
}

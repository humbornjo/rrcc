package rrcc_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/humbornjo/rrcc"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var (
	redisSvr *miniredis.Miniredis
	redisCli *redis.Client
)

func TestMain(m *testing.M) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	redisSvr, _ = miniredis.Run()
	defer redisSvr.Close()
	redisCli = redis.NewClient(
		&redis.Options{Addr: redisSvr.Addr()},
	)
	m.Run()
}

func TestSimple(t *testing.T) {
	rcc, _ := rrcc.FromGetConn(
		context.Background(),
		func() *redis.Client { return redisCli },
	)
	value := "some val that needs to be watched"

	// create a poller and start watching
	key := "TEST_KEY"
	wg := sync.WaitGroup{}
	poller := rcc.Data(key)
	poller.Watch(func(e rrcc.Event) {
		switch e.Type {
		case rrcc.DEL:
		// handle delete Event
		case rrcc.ADD, rrcc.CHG, rrcc.PING:
			if e.NewValue != value {
				value = e.NewValue
				wg.Done()
			}
		}
	})

	// value needs update
	wg.Add(1)
	if err := rcc.Update(key, func() (string, error) {
		return "new value", nil
	}, time.Hour); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
	assert.Equal(t, "new value", value)
}

package rrcc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var (
	redisSvr *miniredis.Miniredis
	redisCli *redis.Client
)

func TestMain(m *testing.M) {
	redisSvr, _ = miniredis.Run()
	defer redisSvr.Close()
	redisCli = redis.NewClient(
		&redis.Options{Addr: redisSvr.Addr()},
	)
	m.Run()
}

func TestConcurrentUpd(t *testing.T) {
	// Do init for the rrcc client
	key := "cheerstothetinman"
	ctx := context.Background()
	client, err := FromGetConn(ctx,
		func() *redis.Client { return redisCli })
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	// Prepare test data
	DataSet := []struct {
		value string
	}{
		{"1 - all work and no play makes jack a dull boy"},
		{"2 - all work and no play makes jack a dull boy"},
		{"3 - all work and no play makes jack a dull boy"},
	}
	Expected := []Event{
		{Type: ADD, Version: 1, NewValue: DataSet[0].value},
		{Type: CHG, Version: 2, NewValue: DataSet[1].value, OldValue: DataSet[0].value},
		{Type: CHG, Version: 3, NewValue: DataSet[2].value, OldValue: DataSet[1].value},
	}

	var wg sync.WaitGroup
	cntUpd := atomic.Int32{}
	epoch := 0
	concurrency := 500
	pollers := make([]poller, 0)
	stepCh := make(chan struct{}, 1)
	closeCh := make(chan struct{}, 1)

	doneChs := make([]chan struct{}, len(Expected))
	for i := range doneChs {
		doneChs[i] = make(chan struct{}, 1)
	}

	go func() {
		for {
			wg.Add(concurrency)
			slog.Info("[WATCH] wg reinitialize", "epoch", epoch)
			stepCh <- struct{}{}
			close(doneChs[epoch])

			wg.Wait()
			epoch += 1
			if epoch == len(Expected) {
				close(closeCh)
				return
			}
		}
	}()

	for i := range concurrency {
		p := client.Data(key)
		pollers = append(pollers, p)
		go func(i int, p poller) {
			p.Watch(func(e Event) {
				slog.Info(fmt.Sprintf("[WATCH] poller %d", i), "epoch", epoch, "event", e)
				if !assert.Equal(t, Expected[epoch], e) {
					t.Fail()
				}
				<-doneChs[epoch]
				wg.Done()
			})
		}(i, p)
	}

encore:
	select {
	case <-closeCh:
	case <-stepCh:
		idx := epoch
		slog.Info("[UPDATE] begin to update", "epoch", idx)
		for _, p := range pollers {
			go func(i int) {
				p.Update(func() string {
					oldV := cntUpd.Load()
					cntUpd.CompareAndSwap(oldV, oldV+1)
					return DataSet[i].value
				})
			}(idx)
		}
		goto encore
	}

	for _, p := range pollers {
		p.Cancel()
	}

	assert.Equal(t, len(Expected), int(cntUpd.Load()))
}

func TestRedisDown(t *testing.T) {
	// Do init for the rrcc client
	key := "nina"
	ctx := context.Background()
	rcc, err := FromGetConn(ctx,
		func() *redis.Client { return redisCli })
	if err != nil {
		t.Fatal(err)
	}
	defer rcc.Stop()

	ch := make(chan struct{})
	once := sync.Once{}
	p := rcc.Data(key)
	p.Watch(func(e Event) {
		switch e.Type {
		case ADD:
			slog.Info("[WATCH] ADD", "event", e)
		case CHG:
			slog.Info("[WATCH] CHG", "event", e)
		}
		once.Do(func() { close(ch) })
	})

	{
		e, err := p.Update(func() string { return "Guns N' Roses" })
		slog.Info("update", "event", e, "err", err)
	}
	<-ch

	// delete all key in redis
	redisCli.FlushDB(ctx)

	<-time.After(10 * time.Second)
	e, err := rcc.(*client)._iclient.Get(ctx, key)
	assert.NoError(t, err)
	slog.Info("event", "event", e)
	assert.Equal(t, e.NewValue, "Guns N' Roses")
}

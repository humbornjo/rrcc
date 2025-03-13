package rrcc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/humbornjo/rrcc/internal/iredis"
	"github.com/redis/go-redis/v9"
)

const (
	testKey = "test_key"
)

var testRedisCfg = redis.Options{
	Addr:     "localhost:6379",
	Username: "defualt",
	Password: "", // no password set
	DB:       0,  // use default DB
}

func TestA(t *testing.T) {
	ctx := context.Background()
	client, err := FromOptions(ctx, testRedisCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	kpoller := client.Data(testKey)
	kpoller.Watch(func(e Event) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("watch update callback panic")
			}
		}()
		switch e.Type {
		case ADD:
			slog.Info("ADD", "event", e)
		case CHG:
			slog.Info("CHG", "event", e)
		case DEL:
			slog.Info("DEL", "event", e)
		}
	})

	time.Sleep(100000 * time.Second)

	kpoller.Update(func() string {
		return time.Now().String()
	})

	time.Sleep(10 * time.Second)

}

func TestAtomicOp(t *testing.T) {
	ctx := context.Background()
	rclient := redis.NewClient(&testRedisCfg)
	iclient := iredis.NewIredis(func() *redis.Client { return rclient })
	resp, err := iclient.AtomicGet(ctx, testKey)
	jstr, _ := json.Marshal(resp)
	fmt.Println(string(jstr), err)

	t.Fail()
}

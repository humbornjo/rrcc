package rrcc

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestA(t *testing.T) {
	ctx := context.Background()
	client, err := FromOptions(ctx, redis.Options{
		Addr:     "localhost:6379",
		Username: "defualt",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	kpoller := client.Data("test_key")
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

	time.Sleep(10 * time.Second)

	kpoller.Update(func() string {
		return "test_value_1"
	})

	time.Sleep(10 * time.Second)

}

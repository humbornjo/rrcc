# RRCC

Redis-based remote config center.

# Idea

Imagine there is a TOKEN, and a API to refresh this TOKEN. Ever'y time you call this API, the token will be refreshed and makes the previous TOKEN immidiately invalid. How to make this TOKEN be used in a distributed way, namely, get utilized and updated by several client simultaneously.

# Usage

```go
// either provide redis config or a func to get redis client
rcc, _ := rrcc.FromGetConn(fn)

key := "TEST_KEY"
value := "some val that needs to be watched"

// create a poller and start watching
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
	// handle error
}

wg.Wait()
assert(value, "new value")
```

# Reference

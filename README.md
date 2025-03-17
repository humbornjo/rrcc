# RRCC

Redis-based remote config center.

# Idea

# Usage

```go
// either provide redis config or a func to get redis client
client, err := rrcc.FromGetConn(fn)
if err != nil {
  // handle error
}
defer client.Stop()


value := "some val that needs to be watched"

// create a poller and start watching
poller := client.Data(key)
poller.Watch(func(e rrcc.Event) {
  switch e.Type {
  case rrcc.DEL:
  // handle delete Event
  case rrcc.ADD, rrcc.CHG:
    if e.Value != value {
      value = e.Value
    }
  }
})


// value needs update
e, err := poller.Update(func() string {
  return "new value"
})

if err != nil {
  // handle error
}

e.NewValue == "new value"
```

# Reference

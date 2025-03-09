# RRCC

Redis-based remote config center.

# Idea

# Usage

```go
// either provide redis config or a func to get redis client
client := rrcc.FromGetConn(fnGetConn)
```

# Reference

1.

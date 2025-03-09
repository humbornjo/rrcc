package rrcc

import "errors"

// Exposed errors
var (
	ErrNilConn    = errors.New("nil redis connection")
	ErrNegTime    = errors.New("invid time duration")
	ErrStopWatch  = errors.New("chan watch closed")
	ErrRedisSetNX = errors.New("redis setnx failed")
)

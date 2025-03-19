package rrcc

import "errors"

// Exposed errors
var (
	// Redis error
	ErrNilConn    = errors.New("Redis connection nil")
	ErrRedisSetNX = errors.New("Redis SETNX failed")

	ErrNegTime         = errors.New("Invalid negative duration")
	ErrStopWatch       = errors.New("Poller already stopped watching")
	ErrRemoteOutOfDate = errors.New("Remote config out-of-date")
)

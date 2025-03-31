package rrcc

import "errors"

// Exposed errors
var (
	// redis error
	ErrNilConn    = errors.New("redis nil connection")
	ErrRedisSetNX = errors.New("redis SETNX failed")

	// rrcc error
	ErrNegTime         = errors.New("negative duration")
	ErrStopWatch       = errors.New("poller not watching")
	ErrRemoteOutOfDate = errors.New("remote config out-of-date")
)

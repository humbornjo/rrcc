package rrcc

import "errors"

// Exposed errors
var (
	ErrNilConn   = errors.New("nil redis connection")
	ErrNegTime   = errors.New("negative time duration, please check your jitter and timeout")
	ErrStopWatch = errors.New("chan watch closed")
)

// Internal errors
var (
	errUnchanged = errors.New("unchanged")
)

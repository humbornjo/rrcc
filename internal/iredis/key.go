package iredis

import (
	"encoding/base64"
	"strings"
)

func (p *WrappedRedis) keyData(key string) string {
	prelude := strings.Join([]string{p.prefix, key}, ":")
	nonce := base64.StdEncoding.EncodeToString([]byte(prelude))
	return strings.Join([]string{prelude, nonce}, ":")
}

func (p *WrappedRedis) keyLock(key string) string {
	return strings.Join([]string{p.keyData(key), "lock"}, ":")
}

func (p *WrappedRedis) keyVersion(key string) string {
	return strings.Join([]string{p.keyData(key), "ver"}, ":")
}

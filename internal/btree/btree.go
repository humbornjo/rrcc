package btree

import (
	"sync"

	"modernc.org/b/v2"
)

type Btree[K any, V any] struct {
	_mu sync.Mutex
	b.Tree[K, V]
}

func (t *Btree[K, V]) Lock() {
	t._mu.Lock()
}

func (t *Btree[K, V]) Unlock() {
	t._mu.Unlock()
}

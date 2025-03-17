package btree

import (
	"sync"

	"modernc.org/b/v2"
)

type Btree[K any, V any] struct {
	sync.RWMutex
	b.Tree[K, V]
}

func NewBtree[K comparable, V any](cmp b.Cmp[K]) Btree[K, V] {
	return Btree[K, V]{
		RWMutex: sync.RWMutex{},
		Tree:    *b.TreeNew[K, V](cmp),
	}
}

func (p *Btree[K, V]) AtomicSet(k K, v V) {
	p.Lock()
	defer p.Unlock()
	p.Set(k, v)
}

func (p *Btree[K, V]) AtomicGet(k K) (V, bool) {
	p.RLock()
	defer p.RUnlock()
	return p.Get(k)
}

func (p *Btree[K, V]) AtomicPut(k K, upd b.Updater[V]) (V, bool) {
	p.Lock()
	defer p.Unlock()
	return p.Put(k, upd)
}

func (p *Btree[K, V]) AtomicDelete(k K) bool {
	p.Lock()
	defer p.Unlock()
	return p.Delete(k)
}

package btree

import (
	"sync"

	"modernc.org/b/v2"
)

type Btree[K any, V any] struct {
	_mu sync.Mutex
	b.Tree[K, V]
}

func NewBtree[K comparable, V any](cmp b.Cmp[K]) Btree[K, V] {
	return Btree[K, V]{
		_mu:  sync.Mutex{},
		Tree: *b.TreeNew[K, V](cmp),
	}
}

func (p *Btree[K, V]) Lock() {
	p._mu.Lock()
}

func (p *Btree[K, V]) Unlock() {
	p._mu.Unlock()
}

func (p *Btree[K, V]) AtomicSet(k K, v V) {
	p._mu.Lock()
	defer p._mu.Unlock()
	p.Set(k, v)
}

func (p *Btree[K, V]) AtomicGet(k K) (V, bool) {
	p._mu.Lock()
	defer p._mu.Unlock()
	return p.Get(k)
}

func (p *Btree[K, V]) AtomicPut(k K, upd b.Updater[V]) (V, bool) {
	p._mu.Lock()
	defer p._mu.Unlock()
	return p.Put(k, upd)
}

func (p *Btree[K, V]) AtomicDelete(k K) bool {
	p._mu.Lock()
	defer p._mu.Unlock()
	return p.Delete(k)
}

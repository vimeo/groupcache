package minigc

import (
	"context"
	"sync"

	"github.com/vimeo/galaxycache/lru"
	"github.com/vimeo/galaxycache/singleflight"
)

type ClusterHydrateCB[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Cluster is an in-process cache intended for in-memory data
// It's a thin wrapper around another cache implementation that takes care of
// filling cache misses instead of requiring Add() calls.
// It leverages the singleflight package to handle cases where hydration can
// fail and/or block.
//
// In contrast, StarSystem is designed for cases where constructing/hydrating/fetching a
// value is quick, can't fail, but not completely free. (It's a lighter-weight implementation)
//
// Note: the underlying cache implementation may change at any time.
type Cluster[K comparable, V any] struct {
	lru       lru.TypedCache[K, V]
	hydrateCB ClusterHydrateCB[K, V]
	sfG       singleflight.TypedGroup[K, V]
	mu        sync.Mutex
}

type ClusterParams[K comparable, V any] struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specificies a callback function to be
	// executed when an typedEntry is purged from the cache.
	OnEvicted func(key K, value V)
}

func NewCluster[K comparable, V any](cb ClusterHydrateCB[K, V], params ClusterParams[K, V]) *Cluster[K, V] {
	return &Cluster[K, V]{
		hydrateCB: cb,
		lru: lru.TypedCache[K, V]{
			MaxEntries: params.MaxEntries,
			OnEvicted:  params.OnEvicted,
		},
	}
}

func (s *Cluster[K, V]) Remove(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lru.Remove(key)
}

func (s *Cluster[K, V]) get(key K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.lru.Get(key); ok {
		return v, true
	}
	var vz V
	return vz, false
}

func (s *Cluster[K, V]) Get(ctx context.Context, key K) (V, error) {
	return s.sfG.Do(key, func() (V, error) {
		if v, ok := s.get(key); ok {
			return v, nil
		}
		val, hydErr := s.hydrateCB(ctx, key)
		if hydErr != nil {
			return val, hydErr
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.lru.Add(key, val)
		return val, nil
	})
}

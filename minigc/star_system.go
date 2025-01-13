// package minigc provides two types for light-weight galaxycache-style
// self-hydrating in-process caches.
//
//   - [StarSystem] is a lightweight cache that uses a single mutex that's held
//     while calling the provided hydration callback. It assumes that hydration
//     cannot fail, so it never returns errors, and Get returns exactly one value.
//   - [Cluster] (short for Star Cluster) leverages the singleflight package to
//     provide locking per-key, so we can release the cluster mutex while calling
//     the hydration callback, making it appropriate for external calls, or
//     anything that can fail.
//
// Unlike a Galaxycache Galaxy, both implementations in this package have
// Remove methods, allowing for limited invalidation.
package minigc

import (
	"context"
	"sync"

	"github.com/vimeo/galaxycache/lru"
)

// StarSystemHydrateCB returns the value for the given key and cannot fail.
type StarSystemHydrateCB[K comparable, V any] func(ctx context.Context, key K) V

// StarSystem is an in-process cache intended for in-memory data
// It's a thin wrapper around another cache implementation that takes care of
// filling cache misses instead of requiring Add() calls.
//
// StarSystem is designed for cases where constructing/hydrating/fetching a
// value is quick, but not completely free. It holds a lock while calling the
// provided callback. For more expensive/blocking calls, one should use StarCluster.
//
// Note: the underlying cache implementation may change at any time.
type StarSystem[K comparable, V any] struct {
	lru       lru.TypedCache[K, V]
	hydrateCB StarSystemHydrateCB[K, V]
	mu        sync.Mutex
}

type StarSystemParams[K comparable, V any] struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specificies a callback function to be
	// executed when an typedEntry is purged from the cache.
	OnEvicted func(key K, value V)
}

func NewStarSystem[K comparable, V any](cb StarSystemHydrateCB[K, V], params StarSystemParams[K, V]) *StarSystem[K, V] {
	return &StarSystem[K, V]{
		hydrateCB: cb,
		lru: lru.TypedCache[K, V]{
			MaxEntries: params.MaxEntries,
			OnEvicted:  params.OnEvicted,
		},
	}
}

func (s *StarSystem[K, V]) Remove(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lru.Remove(key)
}

func (s *StarSystem[K, V]) Get(ctx context.Context, key K) V {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.lru.Get(key); ok {
		return v
	}
	val := s.hydrateCB(ctx, key)
	s.lru.Add(key, val)
	return val
}

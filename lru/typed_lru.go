//go:build go1.18

/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package lru implements an LRU cache.
package lru // import "github.com/vimeo/galaxycache/lru"

// TypedCache is an LRU cache. It is not safe for concurrent access.
type TypedCache[K comparable, V any] struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specificies a callback function to be
	// executed when an typedEntry is purged from the cache.
	OnEvicted func(key K, value V)

	// cache comes first so the GC enqueues marking the map-contents first
	// (which will mark the contents of the linked-list much more
	// efficiently than traversing the linked-list directly)
	cache map[K]*llElem[typedEntry[K, V]]
	ll    linkedList[typedEntry[K, V]]
}

type typedEntry[K comparable, V any] struct {
	key   K
	value V
}

// TypedNew creates a new Cache (with types).
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func TypedNew[K comparable, V any](maxEntries int) *TypedCache[K, V] {
	return &TypedCache[K, V]{
		MaxEntries: maxEntries,
		cache:      make(map[K]*llElem[typedEntry[K, V]]),
	}
}

// Add adds a value to the cache.
func (c *TypedCache[K, V]) Add(key K, value V) {
	if c.cache == nil {
		c.cache = make(map[K]*llElem[typedEntry[K, V]])
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		ele.value.value = value
		return
	}
	ele := c.ll.PushFront(typedEntry[K, V]{key, value})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

// Get looks up a key's value from the cache.
func (c *TypedCache[K, V]) Get(key K) (value V, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.value.value, true
	}
	return
}

// MostRecent returns the most recently used element
func (c *TypedCache[K, V]) MostRecent() *V {
	if c.Len() == 0 {
		return nil
	}
	return &c.ll.Front().value.value
}

// LeastRecent returns the least recently used element
func (c *TypedCache[K, V]) LeastRecent() *V {
	if c.Len() == 0 {
		return nil
	}
	return &c.ll.Back().value.value
}

// Remove removes the provided key from the cache.
func (c *TypedCache[K, V]) Remove(key K) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *TypedCache[K, V]) RemoveOldest() {
	if c.cache == nil {
		return
	}
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *TypedCache[K, V]) removeElement(e *llElem[typedEntry[K, V]]) {
	c.ll.Remove(e)
	kv := e.value
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

// Len returns the number of items in the cache.
func (c *TypedCache[K, V]) Len() int {
	return c.ll.Len()
}

// Clear purges all stored items from the cache.
func (c *TypedCache[K, V]) Clear() {
	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.value
			c.OnEvicted(kv.key, kv.value)
		}
	}
	c.ll = linkedList[typedEntry[K, V]]{}
	c.cache = nil
}

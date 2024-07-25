//go:build go1.19

/*
Copyright 2024 Vimeo Inc.

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

package clock

import "sync/atomic"

type bufferItem[K comparable, V any] struct {
	key   K
	value V
}

type buffer[K comparable, V any] []*bufferItem[K, V]

func (b buffer[K, V]) insertAt(index int, key K, value V) {
	b[index] = &bufferItem[K, V]{
		key:   key,
		value: value,
	}
}

// Cache is a cache based on the CLOCK cache policy.
// It stores elements in a ring buffer, and stores a
// touch count for each element in the cache which
// is uses to determine whether or not a given element
// should be evicted (lower touches means more likely
// to be evicted).
type Cache[K comparable, V any] struct {
	MaxEntries int
	OnEvicted  func(key K, val V)

	indices   map[K]int
	buf       buffer[K, V]
	touches   []atomic.Int64
	clockhand int
}

func New[K comparable, V any](maxEntries int) *Cache[K, V] {
	return &Cache[K, V]{
		MaxEntries: maxEntries,
		indices:    make(map[K]int, maxEntries),
		buf:        make(buffer[K, V], 0, maxEntries),
		touches:    make([]atomic.Int64, maxEntries),
		clockhand:  0,
	}
}

// Add inserts a given key-value pair into the cache,
// evicting a previous entry if necessary
func (c *Cache[K, V]) Add(key K, val V) {
	c.checkAndInit()
	if index, hit := c.indices[key]; hit {
		c.touches[index].Add(1)
		return
	}
	// Not full yet, insert at clock hand
	if !c.IsFull() {
		c.buf = append(c.buf, &bufferItem[K, V]{
			key:   key,
			value: val,
		})
		c.indices[key] = len(c.buf) - 1
	} else {
		// Full, evict by reference bit then replace
		for c.touches[c.clockhand].Load() > 0 {
			c.touches[c.clockhand].Add(-1)
			c.clockhand += 1 % len(c.buf)
		}
		c.Evict(c.buf[c.clockhand].key)
		c.buf.insertAt(c.clockhand, key, val)
		c.indices[key] = c.clockhand
		c.touches[c.clockhand].Add(1)
	}
	c.clockhand = (c.clockhand + 1) % len(c.buf)
}

// Get returns the value for a given key, if present.
// ok bool will be false if the key does not exist
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	c.checkAndInit()
	if index, hit := c.indices[key]; hit {
		c.touches[index].Add(1)
		return c.buf[index].value, true
	}
	return
}

// Evict will remove the given key, if present, from
// the cache
func (c *Cache[K, V]) Evict(key K) {
	c.checkAndInit()
	index, ok := c.indices[key]
	if !ok {
		return
	}
	delete(c.indices, key)
	if c.OnEvicted != nil {
		c.OnEvicted(key, c.buf[index].value)
	}
	c.buf[index] = nil
	c.touches[index].Store(0)
}

// IsFull returns whether or not the cache is at capacity,
// as defined by the cache's max entries
func (c *Cache[K, V]) IsFull() bool {
	return len(c.buf) == c.MaxEntries
}

// checkAndInit ensures that the backing map and slices
// of the cache are not nil
func (c *Cache[K, V]) checkAndInit() {
	if c.indices == nil {
		c.indices = make(map[K]int, c.MaxEntries)
	}
	if c.buf == nil {
		c.buf = make(buffer[K, V], 0, c.MaxEntries)
	}
	if c.touches == nil {
		c.buf = make(buffer[K, V], c.MaxEntries)
	}
}

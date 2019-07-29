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

import (
	"container/list"
	"math"
	"sync"
	"time"
)

// Cache is an LRU cache. It is not safe for concurrent access.
type Cache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specificies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key Key, value interface{})

	ll    *list.List
	cache map[interface{}]*list.Element
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
type Key interface{}

// KeyStats keeps track of the hotness of a key
type KeyStats struct {
	remoteDQPS float64
	dQPS       *dampedQPS
}

func (k *KeyStats) Val() float64 {
	return k.dQPS.val(time.Now())
}

// dampedQPS is an average that recombines the current state with the previous.
type dampedQPS struct {
	sync.Mutex
	period time.Duration
	t      time.Time
	prev   float64
	ct     float64
}

// must be between 0 and 1, the fraction of the new value that comes from
// current rather than previous.
// if `samples` is the number of samples into the damped weighted average you
// want to maximize the fraction of the contribution after; x is the damping
// constant complement (this way we don't have to multiply out (1-x) ^ samples)
// f(x) = (1 - x) * x ^ samples = x ^samples - x ^(samples + 1)
// f'(x) = samples * x ^ (samples - 1) - (samples + 1) * x ^ samples
// this yields a critical point at x = (samples - 1) / samples
const dampingConstant = (1.0 / 30.0) // 5 minutes (30 samples at a 10s interval)
const dampingConstantComplement = 1.0 - dampingConstant

func (a *dampedQPS) incrementHeat(now time.Time) {
	a.Lock()
	defer a.Unlock()
	if a.t.IsZero() {
		a.ct++
		a.t = now
		return
	}
	a.maybeFlush(now)
	a.ct++
}

func (a *dampedQPS) maybeFlush(now time.Time) {
	if now.Sub(a.t) >= a.period {
		prev, cur := a.prev, a.ct
		exponent := math.Floor(float64(now.Sub(a.t))/float64(a.period)) - 1
		a.prev = ((dampingConstant * cur) + (dampingConstantComplement * prev)) * math.Pow(dampingConstantComplement, exponent)
		a.ct = 0
		a.t = now
	}
}

func (a *dampedQPS) val(now time.Time) float64 {
	a.Lock()
	a.maybeFlush(now)
	prev := a.prev
	a.Unlock()
	return prev
}

type entry struct {
	key    Key
	kStats *KeyStats
	value  interface{}
}

// New creates a new Cache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func New(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key Key, value interface{}) {
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		ele.Value.(*entry).value = value
		return
	}
	kStats := &KeyStats{
		dQPS: &dampedQPS{
			period: time.Second,
		},
	}
	ele := c.ll.PushFront(&entry{key, kStats, value})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

// func (c *Cache) InitKeyStats(key string) {
// 	if ele, hit := c.cache[key]; hit {
// 		ele.Value.(*entry).kStats = &KeyStats{}
// 	}
// }

// Get looks up a key's value from the cache and increments its heat.
func (c *Cache) Get(key Key, now time.Time) (value interface{}, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		ele.Value.(*entry).kStats.dQPS.incrementHeat(now)
		return ele.Value.(*entry).value, true
	}
	return
}

func (c *Cache) GetKeyStats(key Key, now time.Time) (kStats *KeyStats, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		ele.Value.(*entry).kStats.dQPS.incrementHeat(now)
		return ele.Value.(*entry).kStats, true
	}
	return
}

// HottestQPS returns the most recently used key
func (c *Cache) HottestQPS(now time.Time) float64 {
	if c == nil {
		return 0
	}
	value := c.ll.Front().Value
	return value.(*entry).kStats.dQPS.val(now)
}

// ColdestQPS returns the least recently used key
func (c *Cache) ColdestQPS(now time.Time) float64 {
	if c == nil {
		return 0
	}
	value := c.ll.Back().Value
	return value.(*entry).kStats.dQPS.val(now)
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key Key) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() {
	if c.cache == nil {
		return
	}
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// Clear purges all stored items from the cache.
func (c *Cache) Clear() {
	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*entry)
			c.OnEvicted(kv.key, kv.value)
		}
	}
	c.ll = nil
	c.cache = nil
}

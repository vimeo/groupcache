//go:build !go1.18

/*
Copyright 2022 Vimeo Inc.

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

package galaxycache

import (
	"sync"

	"github.com/vimeo/galaxycache/lru"
)

type candidateCache struct {
	mu  sync.Mutex
	lru *lru.Cache
}

func newCandidateCache(maxCandidates int) candidateCache {
	return candidateCache{
		lru: lru.New(maxCandidates),
	}
}

func (c *candidateCache) addToCandidateCache(key string, kStats *keyStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Add(key, kStats)
}

func (c *candidateCache) get(key string) (*keyStats, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	val, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	return val.(*keyStats), true
}

// cache is a wrapper around an *lru.Cache that adds synchronization
// and counts the size of all keys and values. Candidate cache only
// utilizes the lru.Cache and mutex, not the included stats.
type cache struct {
	mu         sync.Mutex
	lru        *lru.Cache
	nbytes     int64 // of all keys and values
	nhit, nget int64
	nevict     int64 // number of evictions
	ctype      CacheType
}

func newCache(kind CacheType) cache {
	return cache{
		lru:   lru.New(0),
		ctype: kind,
	}
}

func (c *cache) setLRUOnEvicted(f func(key string, kStats *keyStats)) {
	c.lru.OnEvicted = func(key lru.Key, value interface{}) {
		val := value.(valWithStat)
		c.nbytes -= int64(len(key.(string))) + val.size()
		c.nevict++
		if f != nil {
			f(key.(string), val.stats)
		}
	}
}

func (c *cache) get(key string) (valWithStat, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nget++
	if c.lru == nil {
		return valWithStat{}, false
	}
	vi, ok := c.lru.Get(key)
	if !ok {
		return valWithStat{}, false
	}
	c.nhit++
	return vi.(valWithStat), true
}

func (c *cache) mostRecent() *valWithStat {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.lru.MostRecent()
	val, ok := v.(*valWithStat)
	if !ok {
		return nil
	}
	return val
}

func (c *cache) leastRecent() *valWithStat {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.lru.LeastRecent()
	val, ok := v.(*valWithStat)
	if !ok {
		return nil
	}
	return val
}

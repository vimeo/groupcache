/*
Copyright 2012 Vimeo Inc.

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

// Package galaxycache provides a data loading mechanism with caching
// and de-duplication that works across a set of peer processes.
//
// Each data Get first consults its local cache, otherwise delegates
// to the requested key's canonical owner, which then checks its cache
// or finally gets the data.  In the common case, many concurrent
// cache misses across a set of peers for the same key result in just
// one cache fill.

package galaxycache

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// Promoter is the interface for determining whether a key/value pair should be
// added to the hot cache
type Promoter interface {
	ShouldPromote(key string, data []byte, stats Stats) bool
}

type oneInTenPromoter struct{}

type defaultPromoter struct{}

func (p *oneInTenPromoter) ShouldPromote(key string, data []byte, stats Stats) bool {
	if rand.Intn(10) == 0 {
		return true
	}
	return false
}

func (p *defaultPromoter) ShouldPromote(key string, data []byte, stats Stats) bool {
	keyQPS := stats.KeyQPS
	if keyQPS >= stats.hcStats.ColdestHotQPS {
		return true
	}
	return false
}

type valWithStat struct {
	data  []byte
	stats *keyStats
}

// HCStats keeps track of the size, capacity, and coldest/hottest
// elements in the hot cache
type HCStats struct {
	HottestHotQPS float64
	ColdestHotQPS float64
	HCSize        int64
	HCCapacity    int64
}

func (g *Galaxy) updateHotCacheStats() {
	if g.hotCache.lru == nil {
		g.hotCache.initCache()
	}
	hottestQPS := 0.0
	coldestQPS := 0.0
	hottestEle := g.hotCache.lru.HottestElement(time.Now())
	coldestEle := g.hotCache.lru.ColdestElement(time.Now())
	if hottestEle != nil {
		hottestQPS = hottestEle.(*valWithStat).stats.Val()
		coldestQPS = coldestEle.(*valWithStat).stats.Val()
	}

	newHCS := &HCStats{
		HottestHotQPS: hottestQPS,
		ColdestHotQPS: coldestQPS,
		HCSize:        (g.cacheBytes / g.hcRatio) - g.hotCache.bytes(),
		HCCapacity:    g.cacheBytes / g.hcRatio,
	}
	g.hcStats = newHCS
}

// Stats contains both the KeyQPS and a pointer to the galaxy-wide
// HCStats
type Stats struct {
	KeyQPS  float64
	hcStats *HCStats
}

// keyStats keeps track of the hotness of a key
type keyStats struct {
	dQPS *dampedQPS
}

func newValWithStat(data []byte, kStats *keyStats) *valWithStat {
	value := &valWithStat{
		data: data,
		stats: &keyStats{
			dQPS: &dampedQPS{
				period: time.Second,
			},
		},
	}
	if kStats != nil {
		value.stats = kStats
	}
	return value
}

func (k *keyStats) Val() float64 {
	return k.dQPS.val(time.Now())
}

func (k *keyStats) Touch() {
	k.dQPS.touch(time.Now())
}

// dampedQPS is an average that recombines the current state with the previous.
type dampedQPS struct {
	mu     sync.Mutex
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

func (a *dampedQPS) touch(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
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
	a.mu.Lock()
	a.maybeFlush(now)
	prev := a.prev
	a.mu.Unlock()
	return prev
}

func (g *Galaxy) populateCandidateCache(key string) *keyStats {
	kStats := &keyStats{
		dQPS: &dampedQPS{
			period: time.Second,
		},
	}
	g.candidateCache.addToCandidateCache(key, kStats)
	return kStats
}

func (c *cache) addToCandidateCache(key string, kStats *keyStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Add(key, kStats)
}

/*
Copyright 2019 Vimeo Inc.

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
	"time"

	"github.com/vimeo/galaxycache/promoter"
)

// update the hotcache stats if at least one second has passed since
// last update
func (g *Galaxy) maybeUpdateHotCacheStats() {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	if now.Sub(g.hcStatsWithTime.t) < time.Second {
		return
	}
	nowRel := now.Sub(g.baseTime)
	mruEleQPS := 0.0
	lruEleQPS := 0.0
	mruEle := g.hotCache.mostRecent()
	lruEle := g.hotCache.leastRecent()
	if mruEle != nil { // lru contains at least one element
		_, mruEleQPS = mruEle.stats.val(nowRel)
		_, lruEleQPS = lruEle.stats.val(nowRel)
	}

	newHCS := &promoter.HCStats{
		MostRecentQPS:  mruEleQPS,
		LeastRecentQPS: lruEleQPS,
		HCSize:         (g.cacheBytes / g.opts.hcRatio) - g.hotCache.bytes(),
		HCCapacity:     g.cacheBytes / g.opts.hcRatio,
	}
	g.hcStatsWithTime.hcs = newHCS
	g.hcStatsWithTime.t = now
}

// keyStats keeps track of the hotness of a key
type keyStats struct {
	dQPS windowedAvgQPS
}

func (g *Galaxy) newValWithStat(data []byte, kStats *keyStats) valWithStat {
	if kStats == nil {
		kStats = &keyStats{
			dQPS: windowedAvgQPS{
				trackEpoch: g.now(),
				lastRef:    g.now(),
				count:      0,
			},
		}
	}

	return valWithStat{
		data:  data,
		stats: kStats,
	}
}

func (k *keyStats) val(now time.Duration) (int64, float64) {
	return k.dQPS.val(now)
}

func (k *keyStats) touch(resetIdleAge, now time.Duration) {
	k.dQPS.touch(resetIdleAge, now)
}

type windowedAvgQPS struct {
	// time used for the denominator of the calculation
	// measured relative to the galaxy baseTime
	trackEpoch time.Duration

	// last time the count was updated
	lastRef time.Duration

	count int64

	// protects all above
	mu sync.Mutex
}

func (a *windowedAvgQPS) touch(resetIdleAge, now time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// if the last touch was longer ago than resetIdleAge, pretend this is
	// a new entry.
	// This protects against the case where an entry manages to hang in the
	// cache while idle but only gets hit hard periodically.
	if now-a.lastRef > resetIdleAge {
		a.trackEpoch = now
		a.lastRef = now
		a.count = 1
		return
	}

	if a.lastRef < now {
		// another goroutine may have grabbed a later "now" value
		// before acquiring this lock before this one.
		// Try not to let the timestamp go backwards due to races.
		a.lastRef = now
	}
	a.count++
}

func (a *windowedAvgQPS) val(now time.Duration) (int64, float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	age := now - a.trackEpoch

	// Set a small minimum interval so one request that was super-recent
	// doesn't give a huge rate.
	const minInterval = 100 * time.Millisecond
	if age < minInterval {
		age = minInterval
	}
	return a.count, float64(a.count) / age.Seconds()
}

func (g *Galaxy) addNewToCandidateCache(key string) *keyStats {
	kStats := &keyStats{
		dQPS: windowedAvgQPS{trackEpoch: g.now()},
	}

	g.candidateCache.addToCandidateCache(key, kStats)
	return kStats
}

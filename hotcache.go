/*
Copyright 2012 Google Inc.

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
	stats *KeyStats
}

// HCStats keeps track of the size, capacity, and coldest/hottest
// elements in the hot cache
type HCStats struct {
	HottestHotQPS float64
	ColdestHotQPS float64
	HCSize        int64
	HCCapacity    int64
}

// Stats contains both the KeyQPS and a pointer to the galaxy-wide
// HCStats
type Stats struct {
	KeyQPS  float64
	hcStats *HCStats
}

// KeyStats keeps track of the hotness of a key
type KeyStats struct {
	dQPS *dampedQPS
}

func (k *KeyStats) Val() float64 {
	return k.dQPS.val(time.Now())
}

func (k *KeyStats) LogAccess() {
	k.dQPS.logAccess(time.Now())
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

func (a *dampedQPS) logAccess(now time.Time) {
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

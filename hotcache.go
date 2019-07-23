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

import "math/rand"

// Promoter is the interface for determining whether a key/value pair should be
// added to the hot cache
type Promoter interface {
	ShouldPromote(key string, data []byte, stats KeyStats) bool
}

type defaultPromoter struct{}

func (p *defaultPromoter) ShouldPromote(key string, data []byte, stats KeyStats) bool {
	// TODO(willg): make this smart, use KeyStats
	if rand.Intn(10) == 0 {
		return true
	}
	return false
}

// KeyStats keeps track of the hotness of a key
type KeyStats struct {
	keyQPS       float64
	remoteKeyQPS float64
	hcStats      *HCStats
}

// HCStats keeps track of the size, capacity, and coldest/hottest
// elements in the hot cache
type HCStats struct {
	HottestHotQPS  float64
	ColdestColdQPS float64
	HCSize         int64
	HCCapacity     int64
}

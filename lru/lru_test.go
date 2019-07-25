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

package lru

import (
	"fmt"
	"testing"
	"time"
)

type simpleStruct struct {
	int
	string
}

type complexStruct struct {
	int
	simpleStruct
}

func TestGet(t *testing.T) {
	var getTests = []struct {
		name       string
		keyToAdd   interface{}
		keyToGet   interface{}
		expectedOk bool
	}{
		{"string_hit", "myKey", "myKey", true},
		{"string_miss", "myKey", "nonsense", false},
		{"simple_struct_hit", simpleStruct{1, "two"}, simpleStruct{1, "two"}, true},
		{"simple_struct_miss", simpleStruct{1, "two"}, simpleStruct{0, "noway"}, false},
		{"complex_struct_hit", complexStruct{1, simpleStruct{2, "three"}},
			complexStruct{1, simpleStruct{2, "three"}}, true},
	}

	for _, tt := range getTests {
		lru := New(0)
		lru.Add(tt.keyToAdd, 1234)
		val, ok := lru.Get(tt.keyToGet, time.Now())
		if ok != tt.expectedOk {
			t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
		} else if ok && val != 1234 {
			t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
		}
	}
}

func TestRemove(t *testing.T) {
	lru := New(0)
	lru.Add("myKey", 1234)
	if val, ok := lru.Get("myKey", time.Now()); !ok {
		t.Fatal("TestRemove returned no match")
	} else if val != 1234 {
		t.Fatalf("TestRemove failed.  Expected %d, got %v", 1234, val)
	}

	lru.Remove("myKey")
	if _, ok := lru.Get("myKey", time.Now()); ok {
		t.Fatal("TestRemove returned a removed entry")
	}
}

func TestEvict(t *testing.T) {
	evictedKeys := make([]Key, 0)
	onEvictedFun := func(key Key, value interface{}) {
		evictedKeys = append(evictedKeys, key)
	}

	lru := New(20)
	lru.OnEvicted = onEvictedFun
	for i := 0; i < 22; i++ {
		lru.Add(fmt.Sprintf("myKey%d", i), 1234)
	}

	if len(evictedKeys) != 2 {
		t.Fatalf("got %d evicted keys; want 2", len(evictedKeys))
	}
	if evictedKeys[0] != Key("myKey0") {
		t.Fatalf("got %v in first evicted key; want %s", evictedKeys[0], "myKey0")
	}
	if evictedKeys[1] != Key("myKey1") {
		t.Fatalf("got %v in second evicted key; want %s", evictedKeys[1], "myKey1")
	}
}

func TestHotcache(t *testing.T) {
	var hcTests = []struct {
		name                  string
		numGets               int
		numHeatBursts         int
		secsBetweenHeatBursts time.Duration
		secsToVal             time.Duration
		keyToAdd              interface{}
	}{
		{"heat_burst_1_sec", 10000, 5, 1, 15, simpleStruct{1, "hi"}},
		{"heat_burst_2_secs", 10000, 5, 2, 15, simpleStruct{1, "hi"}},
		{"heat_burst_5_secs", 10000, 5, 5, 15, simpleStruct{1, "hi"}},
		{"heat_burst_30_secs", 10000, 5, 30, 15, simpleStruct{1, "hi"}},
	}

	for _, tc := range hcTests {
		t.Run(tc.name, func(t *testing.T) {
			lru := New(0)
			lru.Add(tc.keyToAdd, 1234)
			now := time.Now()
			for k := 0; k < tc.numHeatBursts; k++ {
				for k := 0; k < tc.numGets; k++ {
					lru.cache[tc.keyToAdd].Value.(*entry).kStats.dAvg.IncrementHeat(now)
					now = now.Add(time.Nanosecond)
				}
				t.Logf("QPS on %d gets in 1 second on burst #%d: %f\n", tc.numGets, k+1, lru.cache[tc.keyToAdd].Value.(*entry).kStats.dAvg.prev)
				now = now.Add(time.Second * tc.secsBetweenHeatBursts)
			}
			val := lru.cache[tc.keyToAdd].Value.(*entry).kStats.dAvg.Val(now)
			t.Logf("QPS after all bursts: %f\n", val)
			now = now.Add(time.Second * 5)
			lru.cache[tc.keyToAdd].Value.(*entry).kStats.dAvg.IncrementHeat(now)
			t.Logf("QPS on 1 additional get after %d seconds: %f\n", 5, lru.cache[tc.keyToAdd].Value.(*entry).kStats.dAvg.prev)
			now = now.Add(time.Second * tc.secsToVal)
			val = lru.cache[tc.keyToAdd].Value.(*entry).kStats.dAvg.Val(now)
			t.Logf("QPS on Val() get after %d seconds: %f\n", tc.secsToVal, val)
		})
	}
}

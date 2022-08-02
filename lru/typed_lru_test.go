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

package lru

import (
	"fmt"
	"testing"
)

func TestTypedGet(t *testing.T) {
	getTests := []struct {
		name       string
		keyToAdd   string
		keyToGet   string
		expectedOk bool
	}{
		{"string_hit", "myKey", "myKey", true},
		{"string_miss", "myKey", "nonsense", false},
	}

	for _, tt := range getTests {
		lru := TypedNew[string, int](0)
		lru.Add(tt.keyToAdd, 1234)
		val, ok := lru.Get(tt.keyToGet)
		if ok != tt.expectedOk {
			t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
		} else if ok && val != 1234 {
			t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
		}
	}
}

func TestTypedRemove(t *testing.T) {
	lru := TypedNew[string, int](0)
	lru.Add("myKey", 1234)
	if val, ok := lru.Get("myKey"); !ok {
		t.Fatal("TestRemove returned no match")
	} else if val != 1234 {
		t.Fatalf("TestRemove failed.  Expected %d, got %v", 1234, val)
	}

	lru.Remove("myKey")
	if _, ok := lru.Get("myKey"); ok {
		t.Fatal("TestRemove returned a removed entry")
	}
}

func TestTypedEvict(t *testing.T) {
	evictedKeys := make([]Key, 0)
	onEvictedFun := func(key string, value int) {
		evictedKeys = append(evictedKeys, key)
	}

	lru := TypedNew[string, int](20)
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
	// move 9 and 10 to the head
	lru.Get("myKey10")
	lru.Get("myKey9")
	// add another few keys to evict the the others
	for i := 22; i < 32; i++ {
		lru.Add(fmt.Sprintf("myKey%d", i), 1234)
	}

}

func BenchmarkTypedGetAllHits(b *testing.B) {
	b.ReportAllocs()
	type complexStruct struct {
		a, b, c, d, e, f int64
		k, l, m, n, o, p float64
	}
	// Populate the cache
	l := TypedNew[int, complexStruct](32)
	for z := 0; z < 32; z++ {
		l.Add(z, complexStruct{a: int64(z)})
	}

	b.ResetTimer()
	for z := 0; z < b.N; z++ {
		// take the lower 5 bits as mod 32 so we always hit
		l.Get(z & 31)
	}
}

func BenchmarkTypedGetHalfHits(b *testing.B) {
	b.ReportAllocs()
	type complexStruct struct {
		a, b, c, d, e, f int64
		k, l, m, n, o, p float64
	}
	// Populate the cache
	l := TypedNew[int, complexStruct](32)
	for z := 0; z < 32; z++ {
		l.Add(z, complexStruct{a: int64(z)})
	}

	b.ResetTimer()
	for z := 0; z < b.N; z++ {
		// take the lower 4 bits as mod 16 shifted left by 1 to
		l.Get((z&15)<<1 | z&16>>4 | z&1<<4)
	}
}

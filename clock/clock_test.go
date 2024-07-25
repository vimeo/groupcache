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

import "testing"

func TestCacheGet(t *testing.T) {
	tests := []struct {
		name       string
		keyToAdd   string
		keyToGet   string
		valueToAdd int
		expectedOk bool
	}{
		{"string_hit", "myKey", "myKey", 1234, true},
		{"string_miss", "myKey", "nonsense", 1234, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := New[string, int](5)
			cache.Add(test.keyToAdd, test.valueToAdd)
			value, ok := cache.Get(test.keyToGet)
			if ok != test.expectedOk {
				t.Fatalf("cache hit = %v; want %v", ok, test.expectedOk)
			} else if ok && value != test.valueToAdd {
				t.Fatalf("got %v; want %v", value, test.valueToAdd)
			}
		})
	}
}

func TestCacheAdd(t *testing.T) {
	tests := []struct {
		name          string
		size          int
		keysToAdd     []string
		valueToAdd    []string
		expectedCache buffer[string, string]
	}{
		{
			"beyond_capacity_evicts_first_untouched",
			3,
			[]string{"key-a", "key-b", "key-c", "key-d", "key-e"},
			[]string{"val-a", "val-b", "val-c", "val-d", "val-e"},
			buffer[string, string]{
				&bufferItem[string, string]{key: "key-e", value: "val-e"},
				&bufferItem[string, string]{key: "key-b", value: "val-b"},
				&bufferItem[string, string]{key: "key-d", value: "val-d"},
			},
		},
		{
			"multiple_touches_decreases_eviction_chances",
			3,
			[]string{"key-a", "key-b", "key-c", "key-a", "key-a", "key-d", "key-e"},
			[]string{"val-a", "val-b", "val-c", "val-a", "val-a", "val-d", "val-e"},
			buffer[string, string]{
				&bufferItem[string, string]{key: "key-a", value: "val-a"},
				&bufferItem[string, string]{key: "key-e", value: "val-e"},
				&bufferItem[string, string]{key: "key-d", value: "val-d"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := New[string, string](test.size)
			for i := 0; i < len(test.keysToAdd); i++ {
				key := test.keysToAdd[i]
				val := test.valueToAdd[i]
				cache.Add(key, val)
			}
			for i := 0; i < len(cache.buf); i++ {
				cachedKey := cache.buf[i].key
				cachedValue := cache.buf[i].value
				expectedKey := test.expectedCache[i].key
				expectedValue := test.expectedCache[i].value
				if cachedKey != expectedKey {
					t.Fatalf("bad cache key; got %s, wanted %s at index %d", cachedKey, expectedKey, i)
				}
				if cachedValue != expectedValue {
					t.Fatalf("bad cache value; got %s, wanted %s at index %d", cachedValue, expectedValue, i)
				}
			}
		})
	}
}

func BenchmarkGetAllHits(b *testing.B) {
	b.ReportAllocs()
	type complexStruct struct {
		a, b, c, d, e, f int64
		k, l, m, n, o, p float64
	}
	// Populate the cache
	l := New[int, complexStruct](32)
	for z := 0; z < 32; z++ {
		l.Add(z, complexStruct{a: int64(z)})
	}

	b.ResetTimer()
	for z := 0; z < b.N; z++ {
		// take the lower 5 bits as mod 32 so we always hit
		l.Get(z & 31)
	}
}

func BenchmarkGetHalfHits(b *testing.B) {
	b.ReportAllocs()
	type complexStruct struct {
		a, b, c, d, e, f int64
		k, l, m, n, o, p float64
	}
	// Populate the cache
	l := New[int, complexStruct](32)
	for z := 0; z < 32; z++ {
		l.Add(z, complexStruct{a: int64(z)})
	}

	b.ResetTimer()
	for z := 0; z < b.N; z++ {
		// take the lower 4 bits as mod 16 shifted left by 1 to
		l.Get((z&15)<<1 | z&16>>4 | z&1<<4)
	}
}

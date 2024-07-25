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

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

// Package consistenthash provides an implementation of a ring hash.
package consistenthash // import "github.com/vimeo/galaxycache/consistenthash"

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash maps the data to a uint32 hash-ring
type Hash func(data []byte) uint32

// Map tracks segments in a hash-ring, mapped to specific keys.
type Map struct {
	hash       Hash
	segsPerKey int
	keyHashes  []uint32 // Sorted
	hashMap    map[uint32]string
	keys       map[string]struct{}
}

// New constructs a new consistenthash hashring, with segsPerKey segments per added key.
func New(segsPerKey int, fn Hash) *Map {
	m := &Map{
		segsPerKey: segsPerKey,
		hash:       fn,
		hashMap:    make(map[uint32]string),
		keys:       make(map[string]struct{}),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.keyHashes) == 0
}

// Add adds some keys to the hashring, establishing ownership of segsPerKey
// segments.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		m.keys[key] = struct{}{}
		for i := 0; i < m.segsPerKey; i++ {
			hash := m.hash([]byte(strconv.Itoa(i) + key))
			m.keyHashes = append(m.keyHashes, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Slice(m.keyHashes, func(i, j int) bool { return m.keyHashes[i] < m.keyHashes[j] })
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := m.hash([]byte(key))

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keyHashes), func(i int) bool { return m.keyHashes[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.keyHashes) {
		idx = 0
	}

	return m.hashMap[m.keyHashes[idx]]
}

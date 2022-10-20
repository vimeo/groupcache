//go:build go1.18

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

package consistenthash

import (
	"math/rand"
	"reflect"
	"testing"
)

func FuzzHashCollision(f *testing.F) {
	hashFunc := func(d []byte) uint32 {
		if len(d) < 2 {
			return uint32(d[0])
		}
		return uint32(d[0] + d[1])
	}
	f.Fuzz(func(t *testing.T, in1, in2, in3, in4 string, seed int64, segments uint) {
		// Skip anything with too many segments
		if segments < 1 || segments > 1<<20 {
			t.Skip()
		}
		src := rand.NewSource(seed)
		r := rand.New(src)
		input := [...]string{in1, in2, in3, in4}
		r.Shuffle(len(input), func(i, j int) { input[i], input[j] = input[j], input[i] })

		hash1 := New(int(segments), hashFunc)
		hash1.Add(input[:]...)

		r.Shuffle(len(input), func(i, j int) { input[i], input[j] = input[j], input[i] })
		hash2 := New(int(segments), hashFunc)
		hash2.Add(input[:]...)

		if !reflect.DeepEqual(hash1.hashMap, hash2.hashMap) {
			t.Errorf("hash maps are not identical: %+v vs %+v", hash1.hashMap, hash2.hashMap)
		}
		if !reflect.DeepEqual(hash1.keys, hash2.keys) {
			t.Errorf("hash keys are not identical: %+v vs %+v", hash1.keys, hash2.keys)
		}
		if !reflect.DeepEqual(hash1.keyHashes, hash2.keyHashes) {
			t.Errorf("hash keys are not identical: %+v vs %+v", hash1.keys, hash2.keys)
		}
	})
}

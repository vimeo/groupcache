/*
Copyright 2012 Google Inc.
Copyright 2025 Vimeo Inc.

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

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import "sync"

// call is an in-flight or completed Do call
type call[R any] struct {
	wg  sync.WaitGroup
	val R
	err error
}

// Group is a compatiblility alias for TypedGroup[string,any], matching the
// previous interface.
type Group = TypedGroup[string, any]

// TypedGroup represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type TypedGroup[K comparable, R any] struct {
	mu sync.Mutex     // protects m
	m  map[K]*call[R] // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *TypedGroup[K, R]) Do(key K, fn func() (R, error)) (R, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[R])
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call[R])
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}

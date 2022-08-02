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

package lru

import "fmt"

// Default-disable paranoid checks so they get compiled out.
// If this const is renamed/moved/updated make sure to update the sed
// expression in the github action. (.github/workflows/go.yml)
const paranoidLL = false

// LinkedList using generics to reduce the number of heap objects
// Used for the LRU stack in TypedCache
type linkedList[T any] struct {
	head *llElem[T]
	tail *llElem[T]
	size int
}

type llElem[T any] struct {
	next, prev *llElem[T]
	value      T
}

func (l *llElem[T]) Next() *llElem[T] {
	return l.next
}

func (l *llElem[T]) Prev() *llElem[T] {
	return l.prev
}

func (l *linkedList[T]) PushFront(val T) *llElem[T] {
	if paranoidLL {
		l.checkHeadTail()
		defer l.checkHeadTail()
	}
	elem := llElem[T]{
		next:  l.head,
		prev:  nil, // first element
		value: val,
	}
	if l.head != nil {
		l.head.prev = &elem
	}
	if l.tail == nil {
		l.tail = &elem
	}
	l.head = &elem
	l.size++

	return &elem
}

func (l *linkedList[T]) MoveToFront(e *llElem[T]) {
	if paranoidLL {
		if e == nil {
			panic("nil element")
		}
		l.checkHeadTail()
		defer l.checkHeadTail()
	}

	if l.head == e {
		// nothing to do
		return
	}

	if e.next != nil {
		// update the previous pointer on the next element
		e.next.prev = e.prev
	}
	if e.prev != nil {
		e.prev.next = e.next
	}
	if l.head != nil {
		l.head.prev = e
	}

	if l.tail == e {
		l.tail = e.prev
	}
	e.next = l.head
	l.head = e
	e.prev = nil
}

func (l *linkedList[T]) checkHeadTail() {
	if !paranoidLL {
		return
	}
	if (l.head != nil) != (l.tail != nil) {
		panic(fmt.Sprintf("invariant failure; nilness mismatch: head: %+v; tail: %+v (size %d)",
			l.head, l.tail, l.size))
	}

	if l.size > 0 && (l.head == nil || l.tail == nil) {
		panic(fmt.Sprintf("invariant failure; head and/or tail nil with %d size: head: %+v; tail: %+v",
			l.size, l.head, l.tail))
	}

	if l.head != nil && (l.head.prev != nil || (l.head.next == nil && l.size != 1)) {
		panic(fmt.Sprintf("invariant failure; head next/prev invalid with %d size: head: %+v; tail: %+v",
			l.size, l.head, l.tail))
	}
	if l.tail != nil && ((l.tail.prev == nil && l.size != 1) || l.tail.next != nil) {
		panic(fmt.Sprintf("invariant failure; tail next/prev invalid with %d size: head: %+v; tail: %+v",
			l.size, l.head, l.tail))
	}
}

func (l *linkedList[T]) Remove(e *llElem[T]) {
	if paranoidLL {
		if e == nil {
			panic("nil element")
		}
		l.checkHeadTail()
		defer l.checkHeadTail()
	}
	if l.tail == e {
		l.tail = e.prev
	}
	if l.head == e {
		l.head = e.next
	}

	if e.next != nil {
		// update the previous pointer on the next element
		e.next.prev = e.prev
	}
	if e.prev != nil {
		e.prev.next = e.next
	}
	l.size--
}

func (l *linkedList[T]) Len() int {
	return l.size
}

func (l *linkedList[T]) Front() *llElem[T] {
	return l.head
}

func (l *linkedList[T]) Back() *llElem[T] {
	return l.tail
}

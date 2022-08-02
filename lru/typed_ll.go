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

func (l *linkedList[T]) Remove(e *llElem[T]) {
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

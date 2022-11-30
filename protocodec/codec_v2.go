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

package protocodec

import "google.golang.org/protobuf/proto"

type pointerMessage[T any] interface {
	comparable
	*T
	proto.Message
}

// NewV2 constructs a CodecV2 zero-value.
// It exists because type-parameter inference is nicer with function-calls than
// struct-literals
func NewV2[C any, T pointerMessage[C]]() CodecV2[C, T] {
	return CodecV2[C, T]{}
}

// CodecV2 wraps a google.golang.org/protobuf/proto.Message and implements Codec
type CodecV2[C any, T pointerMessage[C]] struct {
	msg T
}

func (c *CodecV2[C, T]) empty() bool {
	return (*C)(c.msg) == nil
}

func (c *CodecV2[C, T]) maybeInit() {
	if c.empty() {
		c.msg = new(C)
	}
}

// MarshalBinary on a ProtoCodec returns the encoded proto message
func (c *CodecV2[C, T]) MarshalBinary() ([]byte, error) {
	c.maybeInit()
	return proto.Marshal(c.msg)
}

// UnmarshalBinary on a ProtoCodec unmarshals provided data into
// the proto message
func (c *CodecV2[C, T]) UnmarshalBinary(data []byte) error {
	c.maybeInit()
	return proto.Unmarshal(data, c.msg)
}

// Get implies returns the internal protobuf message value
func (c *CodecV2[C, T]) Get() T {
	return c.msg
}

// Set bypasses Marshaling and lets you set the internal protobuf value
func (c *CodecV2[C, T]) Set(v T) {
	c.msg = v
}

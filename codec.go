/*
Copyright 2012 Google Inc.

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

package galaxycache

import (
	"github.com/golang/protobuf/proto"
)

// Codec includes both the BinaryMarshaler and BinaryUnmarshaler
// interfaces
type Codec interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
}

// ByteCodec is a byte slice type that implements Codec
type ByteCodec []byte

func (c *ByteCodec) MarshalBinary() ([]byte, error) {
	return *c, nil
}

func (c *ByteCodec) UnmarshalBinary(data []byte) error {
	tmp := make([]byte, len(data))
	copy(tmp, data)
	*c = tmp
	return nil
}

// StringCodec is a string type that implements Codec
type StringCodec string

func (c *StringCodec) MarshalBinary() ([]byte, error) {
	return []byte(*c), nil
}

func (c *StringCodec) UnmarshalBinary(data []byte) error {
	*c = StringCodec(data)
	return nil
}

// ProtoCodec wraps a proto.Message and implements Codec
type ProtoCodec struct {
	Msg proto.Message
}

func (c *ProtoCodec) MarshalBinary() ([]byte, error) {
	return proto.Marshal(c.Msg)
}

func (c *ProtoCodec) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, c.Msg)
}

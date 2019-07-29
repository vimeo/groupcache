/*
Copyright 2019 Vimeo Inc.

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

import "github.com/golang/protobuf/proto"

// ProtoCodec wraps a proto.Message and implements Codec
type ProtoCodec struct {
	Msg proto.Message
}

// MarshalBinary on a ProtoCodec returns the encoded proto message
func (c *ProtoCodec) MarshalBinary() ([]byte, error) {
	return proto.Marshal(c.Msg)
}

// UnmarshalBinary on a ProtoCodec unmarshals provided data into
// the proto message
func (c *ProtoCodec) UnmarshalBinary(data []byte) error {
	return proto.Unmarshal(data, c.Msg)
}

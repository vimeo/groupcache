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

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/vimeo/galaxycache/testpb"
)

func TestProtoCodec(t *testing.T) {
	inProtoCodec := &ProtoCodec{
		Msg: &testpb.TestMessage{
			Name: proto.String("TestName"),
			City: proto.String("TestCity"),
		},
	}

	testMsgBytes, err := inProtoCodec.MarshalBinary()
	if err != nil {
		t.Errorf("Error marshaling from protoCodec: %s", err)
	}
	t.Logf("Marshaled Bytes: %q", string(testMsgBytes))

	outProtoCodec := &ProtoCodec{
		Msg: &testpb.TestMessage{},
	}

	if unmarshalErr := outProtoCodec.UnmarshalBinary(testMsgBytes); unmarshalErr != nil {
		t.Errorf("Error unmarshaling: %s", unmarshalErr)
	}

	if !proto.Equal(outProtoCodec.Msg, inProtoCodec.Msg) {
		t.Errorf("UnmarshalBinary resulted in %q; want %q", outProtoCodec.Msg.String(), inProtoCodec.Msg.String())
	}

}

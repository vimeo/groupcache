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

package protocodec_test

import (
	"testing"

	"github.com/vimeo/galaxycache/protocodec"
	"github.com/vimeo/galaxycache/protocodec/internal/testpbv2"
	"google.golang.org/protobuf/proto"
)

func TestProtoCodecV2(t *testing.T) {
	inProtoCodec := protocodec.NewV2[testpbv2.TestMessage]()
	inProtoCodec.Set(&testpbv2.TestMessage{
		Name: proto.String("TestName"),
		City: proto.String("TestCity"),
	})

	testMsgBytes, err := inProtoCodec.MarshalBinary()
	if err != nil {
		t.Errorf("Error marshaling from protoCodec: %s", err)
	}
	t.Logf("Marshaled Bytes: %q", string(testMsgBytes))

	outProtoCodec := protocodec.NewV2[testpbv2.TestMessage]()

	if unmarshalErr := outProtoCodec.UnmarshalBinary(testMsgBytes); unmarshalErr != nil {
		t.Errorf("Error unmarshaling: %s", unmarshalErr)
	}

	if !proto.Equal(outProtoCodec.Get(), inProtoCodec.Get()) {
		t.Errorf("UnmarshalBinary resulted in %q; want %q", outProtoCodec.Get().String(), inProtoCodec.Get().String())
	}
}

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

package galaxycache

import "testing"

func TestByteCodecTarget(t *testing.T) {
	var byteCodec ByteCodec

	inBytes := []byte("some bytes")
	byteCodec.UnmarshalBinary(inBytes)
	if want := "some bytes"; string(byteCodec) != want {
		t.Errorf("UnmarshalBinary resulted in %q; want %q", byteCodec, want)
	}
	if &inBytes[0] == &byteCodec[0] {
		t.Error("inBytes and byteCodec share memory")
	}

	marshaledBytes, err := byteCodec.MarshalBinary()
	if err != nil {
		t.Errorf("Error marshaling from byteCodec: %s", err)
	}
	if string(marshaledBytes) != string(inBytes) {
		t.Errorf("MarshalBinary resulted in %q; want %q", marshaledBytes, inBytes)
	}
	if &inBytes[0] == &marshaledBytes[0] {
		t.Errorf("inBytes and marshaledBytes share memory")
	}

}

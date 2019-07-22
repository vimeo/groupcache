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

const testBytes = "some bytes"

func TestCodec(t *testing.T) {
	var byteCodec ByteCodec
	var copyingByteCodec CopyingByteCodec
	var stringCodec StringCodec

	testCases := []struct {
		testName string
		codec    Codec
	}{
		{
			testName: "ByteCodec",
			codec:    &byteCodec,
		},
		{
			testName: "CopyingByteCodec",
			codec:    &copyingByteCodec,
		},
		{
			testName: "StringCodec",
			codec:    &stringCodec,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			inBytes := []byte(testBytes)
			tc.codec.UnmarshalBinary(inBytes)
			inBytes[0] = 'a' // change the original byte slice to ensure copy was made
			if string(tc.codec.GetBytes()) != testBytes {
				t.Errorf("UnmarshalBinary resulted in %q; want %q", tc.codec, testBytes)
			}

			marshaledBytes, err := tc.codec.MarshalBinary()
			if err != nil {
				t.Errorf("Error marshaling from byteCodec: %s", err)
			}
			if string(marshaledBytes) != testBytes {
				t.Errorf("MarshalBinary resulted in %q; want %q", marshaledBytes, testBytes)
			}
			if &inBytes[0] == &marshaledBytes[0] {
				t.Errorf("inBytes and marshaledBytes share memory")
			}

			if tc.testName == "CopyingByteCodec" {
				codecBytes := tc.codec.GetBytes()
				if &marshaledBytes[0] == &codecBytes[0] {
					t.Errorf("Marshaling did not copy the bytes")
				}
			}
		})
	}
}

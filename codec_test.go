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

import (
	"bytes"
	"testing"
)

const testBytes = "some bytes"

func TestCodec(t *testing.T) {
	var stringCodec StringCodec

	testCases := []struct {
		testName  string
		codec     Codec
		checkCopy bool
	}{
		{
			testName:  "ByteCodec",
			codec:     &ByteCodec{},
			checkCopy: false,
		},
		{
			testName:  "CopyingByteCodec",
			codec:     &CopyingByteCodec{},
			checkCopy: true,
		},
		{
			testName:  "StringCodec",
			codec:     &stringCodec,
			checkCopy: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			inBytes := []byte(testBytes)
			tc.codec.UnmarshalBinary(inBytes)
			inBytes[0] = 'a' // change the original byte slice to ensure copy was made
			marshaledBytes, err := tc.codec.MarshalBinary()
			if err != nil {
				t.Errorf("Error marshaling from byteCodec: %s", err)
			}
			if string(marshaledBytes) != testBytes {
				t.Errorf("Unmarshal/Marshal resulted in %q; want %q", marshaledBytes, testBytes)
			}

			if tc.checkCopy {
				marshaledBytes[0] = 'a'
				secondMarshaledBytes, errM := tc.codec.MarshalBinary()
				if errM != nil {
					t.Errorf("Error marshaling from byteCodec: %s", errM)
				}
				if bytes.Equal(marshaledBytes, secondMarshaledBytes) {
					t.Errorf("Marshaling did not copy the bytes")
				}
			}
		})
	}
}

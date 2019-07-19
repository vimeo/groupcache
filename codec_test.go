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

func TestByteCodec(t *testing.T) {
	var byteCodec ByteCodec

	inBytes := []byte(testBytes)
	byteCodec.UnmarshalBinary(inBytes)
	if string(byteCodec) != testBytes {
		t.Errorf("UnmarshalBinary resulted in %q; want %q", byteCodec, testBytes)
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

func TestCopyingByteCodec(t *testing.T) {
	var copyingByteCodec CopyingByteCodec

	inBytes := []byte(testBytes)
	copyingByteCodec.UnmarshalBinary(inBytes)
	if string(copyingByteCodec) != testBytes {
		t.Errorf("UnmarshalBinary resulted in %q; want %q", copyingByteCodec, testBytes)
	}
	if &inBytes[0] == &copyingByteCodec[0] {
		t.Error("inBytes and copyingByteCodec share memory")
	}

	marshaledBytes, err := copyingByteCodec.MarshalBinary()
	if err != nil {
		t.Errorf("Error marshaling from copyingByteCodec: %s", err)
	}
	if string(marshaledBytes) != string(inBytes) {
		t.Errorf("MarshalBinary resulted in %q; want %q", marshaledBytes, inBytes)
	}
	if &inBytes[0] == &marshaledBytes[0] {
		t.Errorf("inBytes and marshaledBytes share memory")
	}
	if &marshaledBytes[0] == &copyingByteCodec[0] {
		t.Errorf("Marshaling did not copy the bytes")
	}

}

func TestStringCodec(t *testing.T) {
	var stringCodec StringCodec

	inBytes := []byte(testBytes)
	stringCodec.UnmarshalBinary(inBytes)
	inBytes[0] = 'a' // change the original byte slice to ensure copy was made
	if string(stringCodec) != testBytes {
		t.Errorf("UnmarshalBinary resulted in %q; want %q", stringCodec, testBytes)
	}

	marshaledStringBytes, err := stringCodec.MarshalBinary()
	if err != nil {
		t.Errorf("Error marshaling from stringCodec: %s", err)
	}
	if string(marshaledStringBytes) != testBytes {
		t.Errorf("MarshalBinary resulted in %q; want %q", marshaledStringBytes, testBytes)
	}
	if &inBytes[0] == &marshaledStringBytes[0] {
		t.Errorf("inBytes and marshaledStringBytes share memory")
	}

}

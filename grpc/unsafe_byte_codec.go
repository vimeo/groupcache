package grpc

import "time"

// unsafeByteCodec is a byte slice type that implements Codec
type unsafeByteCodec struct {
	bytes  []byte
	expire time.Time
}

// MarshalBinary returns the contained byte-slice
func (c *unsafeByteCodec) MarshalBinary() ([]byte, time.Time, error) {
	return c.bytes, c.expire, nil
}

// UnmarshalBinary to provided data so they share the same backing array
// this is a generally unsafe performance optimization, but safe in the context
// of the gRPC server.
func (c *unsafeByteCodec) UnmarshalBinary(data []byte, expire time.Time) error {
	c.bytes = data
	c.expire = expire
	return nil
}

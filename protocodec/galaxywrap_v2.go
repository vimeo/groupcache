//go:build go1.18

package protocodec

import (
	"context"

	"github.com/vimeo/galaxycache"
)

// GalaxyGet is a simple wrapper around a Galaxy.Get method-call that takes
// care of constructing the protocodec.CodecV2, etc. (making the interface more idiomatic for Go)
func GalaxyGet[C any, T pointerMessage[C]](ctx context.Context, g *galaxycache.Galaxy, key string) (m T, getErr error) {
	pc := CodecV2[C, T]{}
	getErr = g.Get(ctx, key, &pc)
	if getErr != nil {
		return // use named return values to bring the inlining cost down
	}
	return pc.msg, nil
}

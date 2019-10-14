/*
Copyright 2013 Google Inc.

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

package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	gc "github.com/vimeo/galaxycache"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
)

type testStatsExporter struct {
	mu   sync.Mutex
	data []*view.Data
	t    *testing.T
}

func TestHTTPHandler(t *testing.T) {

	const (
		nRoutines = 5
		nGets     = 100
	)

	for _, galaxyNameLoop := range []string{"peerFetchTest", "peerFetchTestWithSlash/foobar"} {
		galaxyName := galaxyNameLoop
		t.Run(galaxyName, func(t *testing.T) {

			var peerAddresses []string
			var peerListeners []net.Listener

			for i := 0; i < nRoutines; i++ {
				newListener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				peerAddresses = append(peerAddresses, newListener.Addr().String())
				peerListeners = append(peerListeners, newListener)
			}

			universe := gc.NewUniverse(NewHTTPFetchProtocol(nil), "shouldBeIgnored")
			serveMux := http.NewServeMux()
			RegisterHTTPHandler(universe, nil, serveMux)
			err := universe.Set(peerAddresses...)
			if err != nil {
				t.Errorf("Error setting peers: %s", err)
			}

			getter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Codec) error {
				return fmt.Errorf("oh no! Local get occurred")
			})
			g := universe.NewGalaxy(galaxyName, 1<<20, getter)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			for _, listener := range peerListeners {
				go makeHTTPServerUniverse(ctx, t, galaxyName, peerAddresses, listener)
			}

			for _, key := range testKeys(nGets) {
				var value gc.StringCodec
				if err := g.Get(ctx, key, &value); err != nil {
					t.Fatal(err)
				}
				if suffix := ":" + key; !strings.HasSuffix(string(value), suffix) {
					t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
				}
				t.Logf("Get key=%q, value=%q (peer:key)", key, value)
			}
			// Try it again, this time with a slash in the middle to ensure we're
			// handling those characters properly
			for _, key := range testKeys(nGets) {
				var value gc.StringCodec
				testKey := key + "/" + key
				if err := g.Get(ctx, testKey, &value); err != nil {
					t.Fatal(err)
				}
				if suffix := ":" + testKey; !strings.HasSuffix(string(value), suffix) {
					t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
				}
				t.Logf("Get key=%q, value=%q (peer:key)", testKey, value)
			}

		})
	}
}

func makeHTTPServerUniverse(ctx context.Context, t testing.TB, galaxyName string, addresses []string, listener net.Listener) {
	universe := gc.NewUniverse(NewHTTPFetchProtocol(nil), listener.Addr().String())
	serveMux := http.NewServeMux()
	wrappedHandler := &ochttp.Handler{Handler: serveMux}
	RegisterHTTPHandler(universe, nil, serveMux)
	err := universe.Set(addresses...)
	if err != nil {
		t.Errorf("Error setting peers: %s", err)
	}
	getter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Codec) error {
		dest.UnmarshalBinary([]byte(":" + key))
		return nil
	})
	universe.NewGalaxy(galaxyName, 1<<20, getter)
	newServer := http.Server{Handler: wrappedHandler}
	go func() {
		err := newServer.Serve(listener)
		if err != http.ErrServerClosed {
			t.Errorf("serve failed: %s", err)
		}
	}()

	<-ctx.Done()
	newServer.Shutdown(ctx)
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}

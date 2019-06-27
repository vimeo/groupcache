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

package galaxycache

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

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

	var peerAddresses []string
	var peerListeners []net.Listener

	for i := 0; i < nRoutines; i++ {
		newListener := pickFreeAddr(t)
		peerAddresses = append(peerAddresses, newListener.Addr().String())
		peerListeners = append(peerListeners, newListener)
	}

	universe := NewUniverse(NewHTTPFetchProtocol(nil), "shouldBeIgnored")
	serveMux := http.NewServeMux()
	RegisterHTTPHandler(universe, nil, serveMux)
	universe.Set(addrToURL(peerAddresses)...)

	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		return fmt.Errorf("oh no! Local get occurred")
	})
	g := universe.NewGalaxy("peerFetchTest", 1<<20, getter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, listener := range peerListeners {
		go makeServerUniverse(ctx, peerAddresses, listener)
	}

	for _, key := range testKeys(nGets) {
		var value string
		if err := g.Get(ctx, key, StringSink(&value)); err != nil {
			t.Fatal(err)
		}
		if suffix := ":" + key; !strings.HasSuffix(value, suffix) {
			t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
		}
		t.Logf("Get key=%q, value=%q (peer:key)", key, value)
	}

}

func makeServerUniverse(ctx context.Context, addresses []string, listener net.Listener) {
	universe := NewUniverse(NewHTTPFetchProtocol(nil), "http://"+listener.Addr().String())
	serveMux := http.NewServeMux()
	wrappedHandler := &ochttp.Handler{Handler: serveMux}
	RegisterHTTPHandler(universe, nil, serveMux)
	universe.Set(addrToURL(addresses)...)

	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		dest.SetString(":" + key)
		return nil
	})
	universe.NewGalaxy("peerFetchTest", 1<<20, getter)
	newServer := http.Server{Handler: wrappedHandler}
	go func() {
		err := newServer.Serve(listener)
		if err != http.ErrServerClosed {
			log.Fatal("serve failed:", err)
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

func pickFreeAddr(t *testing.T) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}

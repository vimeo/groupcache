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

package groupcache

import (
	"context"
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

func TestHTTPServer(t *testing.T) {
	dummyCtx := context.TODO()

	const (
		nRoutines = 2
		nGets     = 10
	)

	var peerAddresses []string

	// This appears to be succeeding with both the current and first server cacher having the same address...
	for i := 0; i < nRoutines; i++ {
		newAddr := pickFreeAddr(t)
		peerAddresses = append(peerAddresses, newAddr)
	}

	cacher := NewCacher(&HTTPProtocol{BasePath: defaultBasePath}, "http://"+peerAddresses[0])
	cacher.Set(addrToURL(peerAddresses)...)
	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		dest.SetString(":" + key)
		return nil
	})
	g := cacher.NewGroup("peerGetsTest", 1<<20, getter)

	for i, address := range peerAddresses {
		if i == 0 {
			// continue
		}
		go makeServerCacher(peerAddresses, address)
	}

	for _, key := range testKeys(nGets) {
		var value string
		if err := g.Get(dummyCtx, key, StringSink(&value)); err != nil {
			t.Fatal(err)
		}
		if suffix := ":" + key; !strings.HasSuffix(value, suffix) {
			t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
		}
		t.Logf("Get key=%q, value=%q (peer:key)", key, value)
	}

}

func makeServerCacher(addresses []string, selfAddress string) {
	cacher := NewCacher(&HTTPProtocol{BasePath: defaultBasePath}, "http://"+selfAddress)
	cacher.Set(addrToURL(addresses)...)

	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		dest.SetString(":" + key)
		return nil
	})
	cacher.NewGroup("peerGetsTest", 1<<20, getter)

	handler := &ochttp.Handler{Handler: cacher.httpServer}
	log.Fatal(http.ListenAndServe(selfAddress, handler))
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}

// This is racy. Another process could swoop in and steal the port between the
// call to this function and the next listen call. Should be okay though.
// The proper way would be to pass the l.File() as ExtraFiles to the child
// process, and then close your copy once the child starts.
func pickFreeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}

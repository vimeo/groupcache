/*
 Copyright 2019 Vimeo Inc.
 Adapted from https://github.com/golang/groupcache/blob/master/http_test.go

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

package grpc

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"

	gc "github.com/vimeo/galaxycache"

	"google.golang.org/grpc"
)

func TestGRPCPeerServer(t *testing.T) {
	var wg sync.WaitGroup

	const (
		nRoutines = 5
		nGets     = 100
	)

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

	universe := gc.NewUniverse(NewGRPCFetchProtocol(grpc.WithInsecure()), "shouldBeIgnored")
	defer func() {
		shutdownErr := universe.Shutdown()
		if shutdownErr != nil {
			t.Errorf("Error on shutdown: %s", shutdownErr)
		}
	}()

	err := universe.Set(peerAddresses...)
	if err != nil {
		t.Errorf("Error setting peers: %s", err)
	}

	getter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Codec) error {
		return fmt.Errorf("oh no! Local get occurred")
	})
	g := universe.NewGalaxy("peerFetchTest", 1<<20, getter)

	ctx, cancel := context.WithCancel(context.Background())

	for _, listener := range peerListeners {
		go runTestPeerGRPCServer(ctx, t, peerAddresses, listener, &wg)
	}

	for _, key := range testKeys(nGets) {
		var value gc.StringCodec
		if err := g.Get(ctx, key, &value); err != nil {
			t.Fatal(err)
		}
		if string(value) != ":"+key {
			t.Errorf("Unexpected value: Get(%q) = %q, expected %q", key, value, ":"+key)
		}
		t.Logf("Get key=%q, value=%q (peer:key)", key, value)
	}
	cancel()
	wg.Wait()
}

func runTestPeerGRPCServer(ctx context.Context, t testing.TB, addresses []string, listener net.Listener, wg *sync.WaitGroup) {
	universe := gc.NewUniverse(NewGRPCFetchProtocol(grpc.WithInsecure()), listener.Addr().String())
	grpcServer := grpc.NewServer()
	RegisterGRPCServer(universe, grpcServer)
	err := universe.Set(addresses...)
	defer func() {
		shutdownErr := universe.Shutdown()
		if shutdownErr != nil {
			t.Errorf("Error on shutdown: %s", shutdownErr)
		}
	}()
	if err != nil {
		t.Errorf("Error setting peers: %s", err)
	}

	getter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Codec) error {
		dest.UnmarshalBinary([]byte(":" + key))
		return nil
	})
	universe.NewGalaxy("peerFetchTest", 1<<20, getter)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := grpcServer.Serve(listener)
		if err != nil {
			t.Errorf("Serve failed: %s", err)
		}
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}

/*
Adapted from https://github.com/golang/groupcache/blob/master/http_test.go
*/

package galaxycache

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
)

func TestGRPCPeerServer(t *testing.T) {
	dummyCtx := context.TODO()

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

	universe := NewUniverse(NewGRPCFetchProtocol(grpc.WithInsecure()), "shouldBeIgnored")
	defer func() {
		shutdownErr := universe.Shutdown()
		if shutdownErr != nil {
			t.Errorf("Error on shutdown: %s", shutdownErr)
		}
	}()
	grpcServer := grpc.NewServer()
	RegisterGRPCServer(universe, grpcServer)
	err := universe.Set(peerAddresses...)
	if err != nil {
		t.Errorf("Error setting peers: %s", err)
	}

	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		return fmt.Errorf("oh no! Local get occurred")
	})
	g := universe.NewGalaxy("peerFetchTest", 1<<20, getter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, listener := range peerListeners {
		go makeGRPCServerUniverse(ctx, t, peerAddresses, listener)
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
	grpcServer.GracefulStop()
}

func makeGRPCServerUniverse(ctx context.Context, t testing.TB, addresses []string, listener net.Listener) {
	universe := NewUniverse(NewGRPCFetchProtocol(grpc.WithInsecure()), listener.Addr().String())
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

	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		dest.SetString(":" + key)
		return nil
	})
	universe.NewGalaxy("peerFetchTest", 1<<20, getter)
	go func() {
		defer listener.Close()
		err := grpcServer.Serve(listener)
		if err != nil {
			t.Errorf("serve failed: %s", err)
		}
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()
}

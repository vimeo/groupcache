/*
Copyright 2012 Google Inc.

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

// Tests for groupcache.

package groupcache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"

	pb "github.com/vimeo/groupcache/groupcachepb"
	testpb "github.com/vimeo/groupcache/testpb"
)

const (
	stringGroupName = "string-group"
	protoGroupName  = "proto-group"
	testMessageType = "google3/net/groupcache/go/test_proto.TestMessage"
	fromChan        = "from-chan"
	cacheSize       = 1 << 20
)

func testSetupStringGroup(c *Cacher, cacheFills *AtomicInt) (*Group, chan string) {
	// cacheFills is the number of times stringGroup or
	// protoGroup's BackendGetter have been called. Read using the
	// cacheFills function.
	stringc := make(chan string)
	stringGroup := c.NewGroup(stringGroupName, cacheSize, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		if key == fromChan {
			key = <-stringc
		}
		cacheFills.Add(1)
		return dest.SetString("ECHO:" + key)
	}))
	return stringGroup, stringc
}

func testSetupProtoGroup(c *Cacher, cacheFills *AtomicInt) (*Group, chan string) {
	stringc := make(chan string)
	protoGroup := c.NewGroup(protoGroupName, cacheSize, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		if key == fromChan {
			key = <-stringc
		}
		cacheFills.Add(1)
		return dest.SetProto(&testpb.TestMessage{
			Name: proto.String("ECHO:" + key),
			City: proto.String("SOME-CITY"),
		})
	}))
	return protoGroup, stringc
}

// tests that a BackendGetter's Get method is only called once with two
// outstanding callers.  This is the string variant.
func TestGetDupSuppressString(t *testing.T) {
	c := NewCacher(&TestProtocol{}, "test")
	var cacheFills AtomicInt
	dummyCtx := context.TODO()
	stringGroup, stringc := testSetupStringGroup(c, &cacheFills)
	// Start two BackendGetters. The first should block (waiting reading
	// from stringc) and the second should latch on to the first
	// one.
	resc := make(chan string, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var s string
			if err := stringGroup.Get(dummyCtx, fromChan, StringSink(&s)); err != nil {
				resc <- "ERROR:" + err.Error()
				return
			}
			resc <- s
		}()
	}

	// Wait a bit so both goroutines get merged together via
	// singleflight.
	// TODO(bradfitz): decide whether there are any non-offensive
	// debug/test hooks that could be added to singleflight to
	// make a sleep here unnecessary.
	time.Sleep(250 * time.Millisecond)

	// Unblock the first getter, which should unblock the second
	// as well.
	stringc <- "foo"

	for i := 0; i < 2; i++ {
		select {
		case v := <-resc:
			if v != "ECHO:foo" {
				t.Errorf("got %q; want %q", v, "ECHO:foo")
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting on getter #%d of 2", i+1)
		}
	}
}

// tests that a Getter's Get method is only called once with two
// outstanding callers.  This is the proto variant.
func TestGetDupSuppressProto(t *testing.T) {
	c := NewCacher(&TestProtocol{}, "test")
	var cacheFills AtomicInt
	dummyCtx := context.TODO()
	protoGroup, stringc := testSetupProtoGroup(c, &cacheFills)
	// Start two getters. The first should block (waiting reading
	// from stringc) and the second should latch on to the first
	// one.
	resc := make(chan *testpb.TestMessage, 2)
	for i := 0; i < 2; i++ {
		go func() {
			tm := new(testpb.TestMessage)
			if err := protoGroup.Get(dummyCtx, fromChan, ProtoSink(tm)); err != nil {
				tm.Name = proto.String("ERROR:" + err.Error())
			}
			resc <- tm
		}()
	}

	// Wait a bit so both goroutines get merged together via
	// singleflight.
	// TODO(bradfitz): decide whether there are any non-offensive
	// debug/test hooks that could be added to singleflight to
	// make a sleep here unnecessary.
	time.Sleep(250 * time.Millisecond)

	// Unblock the first getter, which should unblock the second
	// as well.
	stringc <- "Fluffy"
	want := &testpb.TestMessage{
		Name: proto.String("ECHO:Fluffy"),
		City: proto.String("SOME-CITY"),
	}
	for i := 0; i < 2; i++ {
		select {
		case v := <-resc:
			if !reflect.DeepEqual(v, want) {
				t.Errorf(" Got: %v\nWant: %v", proto.CompactTextString(v), proto.CompactTextString(want))
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting on getter #%d of 2", i+1)
		}
	}
}

func countFills(f func(), cacheFills AtomicInt) int64 {
	fills0 := cacheFills.Get()
	f()
	return cacheFills.Get() - fills0
}

func TestCaching(t *testing.T) {
	c := NewCacher(&TestProtocol{}, "test")
	var cacheFills AtomicInt
	dummyCtx := context.TODO()
	stringGroup, _ := testSetupStringGroup(c, &cacheFills)
	fills := countFills(func() {
		for i := 0; i < 10; i++ {
			var s string
			if err := stringGroup.Get(dummyCtx, "TestCaching-key", StringSink(&s)); err != nil {
				t.Fatal(err)
			}
		}
	}, cacheFills)
	if fills != 1 {
		t.Errorf("expected 1 cache fill; got %d", fills)
	}
}

func TestCacheEviction(t *testing.T) {
	c := NewCacher(&TestProtocol{}, "test")
	var cacheFills AtomicInt
	dummyCtx := context.TODO()
	stringGroup, _ := testSetupStringGroup(c, &cacheFills)
	testKey := "TestCacheEviction-key"
	getTestKey := func() {
		var res string
		for i := 0; i < 10; i++ {
			if err := stringGroup.Get(dummyCtx, testKey, StringSink(&res)); err != nil {
				t.Fatal(err)
			}
		}
	}
	fills := countFills(getTestKey, cacheFills)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill; got %d", fills)
	}

	evict0 := stringGroup.mainCache.nevict

	// Trash the cache with other keys.
	var bytesFlooded int64
	// cacheSize/len(testKey) is approximate
	for bytesFlooded < cacheSize+1024 {
		var res string
		key := fmt.Sprintf("dummy-key-%d", bytesFlooded)
		stringGroup.Get(dummyCtx, key, StringSink(&res))
		bytesFlooded += int64(len(key) + len(res))
	}
	evicts := stringGroup.mainCache.nevict - evict0
	if evicts <= 0 {
		t.Errorf("evicts = %v; want more than 0", evicts)
	}

	// Test that the key is gone.
	fills = countFills(getTestKey, cacheFills)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill after cache trashing; got %d", fills)
	}
}

// type fakePeer struct {
// 	hits int
// 	fail bool
// }

// func (p *fakePeer) Fetch(_ context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
// 	p.hits++
// 	if p.fail {
// 		return errors.New("simulated error from peer")
// 	}
// 	out.Value = []byte("got:" + in.GetKey())
// 	return nil
// }

type TestProtocol struct{}
type TestFetcher struct {
	hits int
	fail bool
}
type testFetchers []RemoteFetcher

func (fetcher *TestFetcher) Fetch(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	// fmt.Println("Fetching!")
	fetcher.hits++
	fmt.Println("Hits for fetcher:", fetcher.hits)
	if fetcher.fail {
		return errors.New("simulated error from peer")
	}
	out.Value = []byte("got:" + in.GetKey())
	return nil
}

func (proto *TestProtocol) NewFetcher(url string, basePath string) RemoteFetcher {
	return &TestFetcher{
		hits: 0,
		fail: false,
	}
}

func TestPeers(t *testing.T) {
	// instantiate test fetchers with the test protocol
	c := NewCacher(&TestProtocol{}, "test")
	dummyCtx := context.TODO()
	rand.Seed(123)
	fetcher0 := &TestFetcher{}
	fetcher1 := &TestFetcher{}
	fetcher2 := &TestFetcher{}
	fetcherList := testFetchers([]RemoteFetcher{fetcher0, fetcher1, fetcher2, nil})

	c.peerPicker.fetchers = map[string]RemoteFetcher{"fetcher0": fetcher0, "fetcher1": fetcher1, "fetcher2": fetcher2}
	c.peerPicker.Set("fetcher0", "fetcher1", "fetcher2")
	c.peerPicker.pickPeerFunc = func(key string, fetchers []RemoteFetcher) (RemoteFetcher, bool) {
		keyNum, err := strconv.Atoi(key)
		if err != nil {
			return &TestFetcher{}, false
		}
		fetchers = fetcherList
		keyIndex := keyNum % len(fetcherList)

		return fetcherList[keyIndex], fetcherList[keyIndex] != nil

	}

	const cacheSize = 0 // empty to force gets from peers rather than local (?)
	localHits := 0
	getter := func(_ context.Context, key string, dest Sink) error {
		localHits++
		fmt.Println("localHits:", localHits)
		return dest.SetString("got:" + key)
	}

	testGroup := c.NewGroup("TestPeers-group", cacheSize, GetterFunc(getter))

	run := func(name string, n int, wantSummary string) {
		// Reset counters
		localHits = 0
		for _, p := range []*TestFetcher{fetcher0, fetcher1, fetcher2} {
			p.hits = 0
		}

		for i := 0; i < n; i++ {
			key := fmt.Sprintf("%d", i)
			want := "got:" + key
			var got string
			err := testGroup.Get(dummyCtx, key, StringSink(&got))
			if err != nil {
				t.Errorf("%s: error on key %q: %v", name, key, err)
				continue
			}
			if got != want {
				t.Errorf("%s: for key %q, got %q; want %q", name, key, got, want)
			}
		}
		summary := func() string {
			return fmt.Sprintf("localHits = %d, peers = %d %d %d", localHits, fetcher0.hits, fetcher1.hits, fetcher2.hits)
		}
		if got := summary(); got != wantSummary {
			t.Errorf("%s: got %q; want %q", name, got, wantSummary)
		}
	}

	resetCacheSize := func(maxBytes int64) {
		g := testGroup
		g.cacheBytes = maxBytes
		g.mainCache = cache{}
		g.hotCache = cache{}
	}

	// Base case; peers all up, with no problems.
	resetCacheSize(1 << 20)
	run("base", 200, "localHits = 50, peers = 50 50 50")

	// Verify cache was hit.  All localHits are gone, and some of
	// the peer hits (the ones randomly selected to be maybe hot)
	// run("cached_base", 200, "localHits = 0, peers = 49 47 48")
	// resetCacheSize(0)

	// // With one of the peers being down.
	// // TODO(bradfitz): on a peer number being unavailable, the
	// // consistent hashing should maybe keep trying others to
	// // spread the load out. Currently it fails back to local
	// // execution if the first consistent-hash slot is unavailable.
	// fetcherList[0] = nil
	// run("one_peer_down", 200, "localHits = 100, peers = 0 49 51")

	// // Failing peer
	// fetcherList[0] = fetcher0
	// fetcher0.fail = true
	// run("peer0_failing", 200, "localHits = 100, peers = 51 49 51")
}

// type fakePeers []RemoteFetcher

// func (p fakePeers) PickPeer(key string) (peer RemoteFetcher, ok bool) {
// 	if len(p) == 0 {
// 		return
// 	}
// 	n := crc32.Checksum([]byte(key), crc32.IEEETable) % uint32(len(p))
// 	return p[n], p[n] != nil
// }

// tests that peers (virtual, in-process) are hit, and how much.
// willg: This test won't work for new_PeerPicker struct implementation in Cacher, since the above PickPeer method won't mean anything without a PeerPicker interface
// func TestPeers(t *testing.T) {
// 	once.Do(testSetup)
// 	rand.Seed(123)
// 	peer0 := &fakePeer{}
// 	peer1 := &fakePeer{}
// 	peer2 := &fakePeer{}
// 	peerList := fakePeers([]RemoteFetcher{peer0, peer1, peer2, nil})
// 	const cacheSize = 0 // disabled
// 	localHits := 0
// 	getter := func(_ context.Context, key string, dest Sink) error {
// 		localHits++
// 		return dest.SetString("got:" + key)
// 	}
// 	testGroup := c.newGroup("TestPeers-group", cacheSize, GetterFunc(getter), peerList)
// 	run := func(name string, n int, wantSummary string) {
// 		// Reset counters
// 		localHits = 0
// 		for _, p := range []*fakePeer{peer0, peer1, peer2} {
// 			p.hits = 0
// 		}

// 		for i := 0; i < n; i++ {
// 			key := fmt.Sprintf("key-%d", i)
// 			want := "got:" + key
// 			var got string
// 			err := testGroup.Get(dummyCtx, key, StringSink(&got))
// 			if err != nil {
// 				t.Errorf("%s: error on key %q: %v", name, key, err)
// 				continue
// 			}
// 			if got != want {
// 				t.Errorf("%s: for key %q, got %q; want %q", name, key, got, want)
// 			}
// 		}
// 		summary := func() string {
// 			return fmt.Sprintf("localHits = %d, peers = %d %d %d", localHits, peer0.hits, peer1.hits, peer2.hits)
// 		}
// 		if got := summary(); got != wantSummary {
// 			t.Errorf("%s: got %q; want %q", name, got, wantSummary)
// 		}
// 	}
// 	resetCacheSize := func(maxBytes int64) {
// 		g := testGroup
// 		g.cacheBytes = maxBytes
// 		g.mainCache = cache{}
// 		g.hotCache = cache{}
// 	}

// 	// Base case; peers all up, with no problems.
// 	resetCacheSize(1 << 20)
// 	run("base", 200, "localHits = 49, peers = 51 49 51")

// 	// Verify cache was hit.  All localHits are gone, and some of
// 	// the peer hits (the ones randomly selected to be maybe hot)
// 	run("cached_base", 200, "localHits = 0, peers = 49 47 48")
// 	resetCacheSize(0)

// 	// With one of the peers being down.
// 	// TODO(bradfitz): on a peer number being unavailable, the
// 	// consistent hashing should maybe keep trying others to
// 	// spread the load out. Currently it fails back to local
// 	// execution if the first consistent-hash slot is unavailable.
// 	peerList[0] = nil
// 	run("one_peer_down", 200, "localHits = 100, peers = 0 49 51")

// 	// Failing peer
// 	peerList[0] = peer0
// 	peer0.fail = true
// 	run("peer0_failing", 200, "localHits = 100, peers = 51 49 51")
// }

func TestTruncatingByteSliceTarget(t *testing.T) {
	c := NewCacher(&TestProtocol{}, "test")
	var cacheFills AtomicInt
	dummyCtx := context.TODO()
	stringGroup, _ := testSetupStringGroup(c, &cacheFills)
	buf := make([]byte, 100)
	s := buf[:]
	sink := TruncatingByteSliceSink(&s)
	println("sink: ", sink)
	if err := stringGroup.Get(dummyCtx, "short", TruncatingByteSliceSink(&s)); err != nil {
		fmt.Println("Err 1: ", err)
		t.Fatal(err)
	}
	if want := "ECHO:short"; string(s) != want {
		fmt.Println("Err 1: ")
		t.Errorf("short key got %q; want %q", s, want)
	}

	s = buf[:6]
	if err := stringGroup.Get(dummyCtx, "truncated", TruncatingByteSliceSink(&s)); err != nil {
		t.Fatal(err)
	}
	if want := "ECHO:t"; string(s) != want {
		t.Errorf("truncated key got %q; want %q", s, want)
	}
}

func TestAllocatingByteSliceTarget(t *testing.T) {
	var dst []byte
	sink := AllocatingByteSliceSink(&dst)

	inBytes := []byte("some bytes")
	sink.SetBytes(inBytes)
	if want := "some bytes"; string(dst) != want {
		t.Errorf("SetBytes resulted in %q; want %q", dst, want)
	}
	v, err := sink.view()
	if err != nil {
		t.Fatalf("view after SetBytes failed: %v", err)
	}
	if &inBytes[0] == &dst[0] {
		t.Error("inBytes and dst share memory")
	}
	if &inBytes[0] == &v.b[0] {
		t.Error("inBytes and view share memory")
	}
	if &dst[0] == &v.b[0] {
		t.Error("dst and view share memory")
	}
}

// orderedFlightGroup allows the caller to force the schedule of when
// orig.Do will be called.  This is useful to serialize calls such
// that singleflight cannot dedup them.
type orderedFlightGroup struct {
	mu     sync.Mutex
	stage1 chan bool
	stage2 chan bool
	orig   flightGroup
}

func (g *orderedFlightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	<-g.stage1
	<-g.stage2
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.orig.Do(key, fn)
}

// TestNoDedup tests invariants on the cache size when singleflight is
// unable to dedup calls.
func TestNoDedup(t *testing.T) {
	c := NewCacher(&TestProtocol{}, "test")
	dummyCtx := context.TODO()
	const testkey = "testkey"
	const testval = "testval"
	g := c.newGroup("testgroup", 1024, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		return dest.SetString(testval)
	}), nil)

	orderedGroup := &orderedFlightGroup{
		stage1: make(chan bool),
		stage2: make(chan bool),
		orig:   g.loadGroup,
	}
	// Replace loadGroup with our wrapper so we can control when
	// loadGroup.Do is entered for each concurrent request.
	g.loadGroup = orderedGroup

	// Issue two idential requests concurrently.  Since the cache is
	// empty, it will miss.  Both will enter load(), but we will only
	// allow one at a time to enter singleflight.Do, so the callback
	// function will be called twice.
	resc := make(chan string, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var s string
			if err := g.Get(dummyCtx, testkey, StringSink(&s)); err != nil {
				resc <- "ERROR:" + err.Error()
				return
			}
			resc <- s
		}()
	}

	// Ensure both goroutines have entered the Do routine.  This implies
	// both concurrent requests have checked the cache, found it empty,
	// and called load().
	orderedGroup.stage1 <- true
	orderedGroup.stage1 <- true
	orderedGroup.stage2 <- true
	orderedGroup.stage2 <- true

	for i := 0; i < 2; i++ {
		if s := <-resc; s != testval {
			t.Errorf("result is %s want %s", s, testval)
		}
	}

	const wantItems = 1
	if g.mainCache.items() != wantItems {
		t.Errorf("mainCache has %d items, want %d", g.mainCache.items(), wantItems)
	}

	// If the singleflight callback doesn't double-check the cache again
	// upon entry, we would increment nbytes twice but the entry would
	// only be in the cache once.
	const wantBytes = int64(len(testkey) + len(testval))
	if g.mainCache.nbytes != wantBytes {
		t.Errorf("cache has %d bytes, want %d", g.mainCache.nbytes, wantBytes)
	}
}

func TestGroupStatsAlignment(t *testing.T) {
	var g Group
	off := unsafe.Offsetof(g.Stats)
	if off%8 != 0 {
		t.Fatal("Stats structure is not 8-byte aligned.")
	}
}

// TODO(bradfitz): port the Google-internal full integration test into here,
// using HTTP requests instead of our RPC system.

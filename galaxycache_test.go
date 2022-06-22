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

// Tests for galaxycache.

package galaxycache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/vimeo/galaxycache/promoter"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	stringGalaxyName = "string-galaxy"
	fromChan         = "from-chan"
	cacheSize        = 1 << 20
)

func initSetup() (*Universe, context.Context, chan string) {
	return NewUniverse(&TestProtocol{}, "test"), context.TODO(), make(chan string)
}

func setupStringGalaxyTest(cacheFills *AtomicInt) (*Galaxy, context.Context, chan string) {
	universe, ctx, stringc := initSetup()
	stringGalaxy := universe.NewGalaxy(stringGalaxyName, cacheSize, GetterFunc(func(_ context.Context, key string, dest Codec) error {
		if key == fromChan {
			key = <-stringc
		}
		cacheFills.Add(1)
		str := "ECHO:" + key
		return dest.UnmarshalBinary([]byte(str), time.Now().Add(5*time.Minute))
	}))
	return stringGalaxy, ctx, stringc
}

// tests that a BackendGetter's Get method is only called once with two
// outstanding callers
func TestGetDupSuppress(t *testing.T) {
	var cacheFills AtomicInt
	stringGalaxy, ctx, stringc := setupStringGalaxyTest(&cacheFills)
	// Start two BackendGetters. The first should block (waiting reading
	// from stringc) and the second should latch on to the first
	// one.
	resc := make(chan string, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var s StringCodec
			if err := stringGalaxy.Get(ctx, fromChan, &s); err != nil {
				resc <- "ERROR:" + err.Error()
				return
			}

			ret, _, err := s.MarshalBinary()
			if err != nil {
				resc <- "ERROR MARSHAL: " + err.Error()
				return
			}
			resc <- string(ret)
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

func countFills(f func(), cacheFills *AtomicInt) int64 {
	fills0 := cacheFills.Get()
	f()
	return cacheFills.Get() - fills0
}

func TestCaching(t *testing.T) {
	var cacheFills AtomicInt
	stringGalaxy, ctx, _ := setupStringGalaxyTest(&cacheFills)
	fills := countFills(func() {
		for i := 0; i < 10; i++ {
			var s StringCodec
			if err := stringGalaxy.Get(ctx, "TestCaching-key", &s); err != nil {
				t.Fatal(err)
			}
		}
	}, &cacheFills)
	if fills != 1 {
		t.Errorf("expected 1 cache fill; got %d", fills)
	}
}

func TestCacheEviction(t *testing.T) {
	var cacheFills AtomicInt
	stringGalaxy, ctx, _ := setupStringGalaxyTest(&cacheFills)
	testKey := "TestCacheEviction-key"
	getTestKey := func() {
		var res StringCodec
		for i := 0; i < 10; i++ {
			if err := stringGalaxy.Get(ctx, testKey, &res); err != nil {
				t.Fatal(err)
			}
		}
	}
	fills := countFills(getTestKey, &cacheFills)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill; got %d", fills)
	}

	evict0 := stringGalaxy.mainCache.nevict

	// Trash the cache with other keys.
	var bytesFlooded int64
	// cacheSize/len(testKey) is approximate
	for bytesFlooded < cacheSize+1024 {
		var res StringCodec
		key := fmt.Sprintf("dummy-key-%d", bytesFlooded)
		require.NoError(t, stringGalaxy.Get(ctx, key, &res))

		ret, _, err := res.MarshalBinary()
		if err != nil {
			t.Fatalf("marshaling binary: %v", err.Error())
		}
		bytesFlooded += int64(len(key) + len(ret))
	}
	evicts := stringGalaxy.mainCache.nevict - evict0
	if evicts <= 0 {
		t.Errorf("evicts = %v; want more than 0", evicts)
	}

	// Test that the key is gone.
	fills = countFills(getTestKey, &cacheFills)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill after cache trashing; got %d", fills)
	}
}

// Testing types to use in TestPeers
type TestProtocol struct {
	TestFetchers map[string]*TestFetcher
}
type TestFetcher struct {
	hits int
	fail bool
}

func (fetcher *TestFetcher) Close() error {
	return nil
}

func (fetcher *TestFetcher) Fetch(ctx context.Context, galaxy string, keys []string) ([]ValueWithTTL, error) {
	if fetcher.fail {
		return []ValueWithTTL{}, errors.New("simulated error from peer")
	}
	fetcher.hits++
	return []ValueWithTTL{
		{
			Data: []byte("got:" + keys[0]),
			TTL:  time.Time{},
		},
	}, nil
}

func (proto *TestProtocol) NewFetcher(url string) (RemoteFetcher, error) {
	newTestFetcher := &TestFetcher{
		hits: 0,
		fail: false,
	}
	proto.TestFetchers[url] = newTestFetcher
	return newTestFetcher, nil
}

// TestPeers tests to ensure that an instance with given hash
// function results in the expected number of gets both locally and into each other peer
func TestPeers(t *testing.T) {

	hashFn := func(data []byte) uint32 {
		dataStr := strings.TrimPrefix(string(data), "0fetcher")
		toReturn, _ := strconv.Atoi(dataStr)
		return uint32(toReturn) % 4
	}
	hashOpts := &HashOptions{
		Replicas: 1,
		HashFn:   hashFn,
	}

	testCases := []struct {
		testName     string
		numGets      int
		expectedHits map[string]int
		cacheSize    int64
		initFunc     func(g *Galaxy, fetchers map[string]*TestFetcher)
	}{
		{
			testName:     "base",
			numGets:      200,
			expectedHits: map[string]int{"fetcher0": 50, "fetcher1": 50, "fetcher2": 50, "fetcher3": 50},
			cacheSize:    1 << 20,
		},
		{
			testName:     "cached_base",
			numGets:      200,
			expectedHits: map[string]int{"fetcher0": 0, "fetcher1": 48, "fetcher2": 47, "fetcher3": 48},
			cacheSize:    1 << 20,
			initFunc: func(g *Galaxy, _ map[string]*TestFetcher) {
				for i := 0; i < 200; i++ {
					key := fmt.Sprintf("%d", i)
					var got StringCodec
					err := g.Get(context.TODO(), key, &got)
					if err != nil {
						t.Errorf("error on key %q: %v", key, err)
						continue
					}
				}
			},
		},
		{
			testName:     "one_peer_down",
			numGets:      200,
			expectedHits: map[string]int{"fetcher0": 100, "fetcher1": 50, "fetcher2": 0, "fetcher3": 50},
			cacheSize:    1 << 20,
			initFunc: func(g *Galaxy, fetchers map[string]*TestFetcher) {
				fetchers["fetcher2"].fail = true
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			// instantiate test fetchers with the test protocol
			testproto := &TestProtocol{
				TestFetchers: make(map[string]*TestFetcher),
			}

			// Initialize source to a deterministic state so we will have
			// predictable hotCache result (given the current 10%-of-the-time
			// method for putting items in the hotCache)
			rand.Seed(123)

			universe := NewUniverseWithOpts(testproto, "fetcher0", hashOpts)
			dummyCtx := context.TODO()

			require.NoError(t, universe.Set("fetcher0", "fetcher1", "fetcher2", "fetcher3"))

			getter := func(_ context.Context, key string, dest Codec) error {
				// these are local hits
				testproto.TestFetchers["fetcher0"].hits++
				return dest.UnmarshalBinary([]byte("got:"+key), time.Now().Add(5*time.Minute))
			}

			testGalaxy := universe.NewGalaxy("TestPeers-galaxy", tc.cacheSize, GetterFunc(getter), WithPromoter(&promoter.ProbabilisticPromoter{ProbDenominator: 10}))

			if tc.initFunc != nil {
				tc.initFunc(testGalaxy, testproto.TestFetchers)
			}

			for _, p := range testproto.TestFetchers {
				p.hits = 0
			}

			for i := 0; i < tc.numGets; i++ {
				key := fmt.Sprintf("%d", i)
				want := "got:" + key
				var got StringCodec
				err := testGalaxy.Get(dummyCtx, key, &got)
				if err != nil {
					t.Errorf("%s: error on key %q: %v", tc.testName, key, err)
					continue
				}

				ret, _, err := got.MarshalBinary()
				if err != nil {
					t.Errorf("%s: error marshaling on key %q: %v", tc.testName, key, err)
					continue
				}
				if string(ret) != want {
					t.Errorf("%s: for key %q, got %q; want %q", tc.testName, key, ret, want)
				}
			}
			for name, fetcher := range testproto.TestFetchers {
				want := tc.expectedHits[name]
				got := fetcher.hits
				if got != want {
					t.Errorf("For %s:  got %d, want %d", name, fetcher.hits, want)
				}

			}
		})
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
	universe, dummyCtx, _ := initSetup()
	const testkey = "testkey"
	const testval = "testval"
	g := universe.NewGalaxy("testgalaxy", 1024, GetterFunc(func(_ context.Context, key string, dest Codec) error {
		return dest.UnmarshalBinary([]byte(testval), time.Now().Add(5*time.Minute))
	}))

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
			var s StringCodec
			if err := g.Get(dummyCtx, testkey, &s); err != nil {
				resc <- "ERROR:" + err.Error()
				return
			}

			ret, _, err := s.MarshalBinary()
			if err != nil {
				resc <- "ERROR MARSHAL:" + err.Error()
				return
			}
			resc <- string(ret)
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
	testKStats := keyStats{dQPS: dampedQPS{period: time.Second}}
	testvws := newValWithStat([]byte(testval), &testKStats, time.Now().Add(1*time.Second))
	wantBytes := int64(len(testkey)) + testvws.size()
	if g.mainCache.nbytes != wantBytes {
		t.Errorf("cache has %d bytes, want %d", g.mainCache.nbytes, wantBytes)
	}
}

func TestGalaxyStatsAlignment(t *testing.T) {
	var g Galaxy
	off := unsafe.Offsetof(g.Stats)
	if off%8 != 0 {
		t.Fatal("Stats structure is not 8-byte aligned.")
	}
}

func TestHotcache(t *testing.T) {
	keyToAdd := "hi"
	hcTests := []struct {
		name             string
		numGets          int
		numHeatBursts    int
		burstInterval    time.Duration
		timeToVal        time.Duration
		expectedBurstQPS float64
		expectedValQPS   float64
	}{
		{
			name:             "10k_heat_burst_1_sec",
			numGets:          10000,
			numHeatBursts:    5,
			burstInterval:    1 * time.Second,
			timeToVal:        5 * time.Second,
			expectedBurstQPS: 1559.0,
			expectedValQPS:   1316.0,
		},
		{
			name:             "10k_heat_burst_5_secs",
			numGets:          10000,
			numHeatBursts:    5,
			burstInterval:    5 * time.Second,
			timeToVal:        5 * time.Second,
			expectedBurstQPS: 1067.0,
			expectedValQPS:   900.5,
		},
		{
			name:             "1k_heat_burst_1_secs",
			numGets:          1000,
			numHeatBursts:    5,
			burstInterval:    1 * time.Second,
			timeToVal:        5 * time.Second,
			expectedBurstQPS: 155.9,
			expectedValQPS:   131.6,
		},
		{
			name:             "1k_heat_burst_5_secs",
			numGets:          1000,
			numHeatBursts:    5,
			burstInterval:    5 * time.Second,
			timeToVal:        5 * time.Second,
			expectedBurstQPS: 106.7,
			expectedValQPS:   90.05,
		},
	}

	for _, tc := range hcTests {
		t.Run(tc.name, func(t *testing.T) {
			u := NewUniverse(&TestProtocol{}, "test-universe")
			g := u.NewGalaxy("test-galaxy", 1<<20, GetterFunc(func(_ context.Context, key string, dest Codec) error {
				return dest.UnmarshalBinary([]byte("hello"), time.Now().Add(5*time.Minute))
			}))
			kStats := &keyStats{
				dQPS: dampedQPS{
					period: time.Second,
				},
			}
			value := newValWithStat([]byte("hello"), kStats, time.Now().Add(1*time.Second))
			g.hotCache.add(keyToAdd, value)
			now := time.Now()
			// blast the key in the hotcache with a bunch of hypothetical gets every few seconds
			for k := 0; k < tc.numHeatBursts; k++ {
				for k := 0; k < tc.numGets; k++ {
					kStats.dQPS.touch(now)
				}
				t.Logf("QPS on %d gets in 1 second on burst #%d: %f\n", tc.numGets, k+1, kStats.dQPS.curDQPS)
				now = now.Add(tc.burstInterval)
			}
			val := kStats.dQPS.val(now)
			if math.Abs(val-tc.expectedBurstQPS) > val/100 { // ensure less than %1 error
				t.Errorf("QPS after bursts: %f, Wanted: %f", val, tc.expectedBurstQPS)
			}
			value2 := newValWithStat([]byte("hello there"), nil, time.Now().Add(1*time.Second))

			g.hotCache.add(keyToAdd+"2", value2) // ensure that hcStats are properly updated after adding
			g.maybeUpdateHotCacheStats()
			t.Logf("Hottest QPS: %f, Coldest QPS: %f\n", g.hcStatsWithTime.hcs.MostRecentQPS, g.hcStatsWithTime.hcs.LeastRecentQPS)

			now = now.Add(tc.timeToVal)
			val = kStats.dQPS.val(now)
			if math.Abs(val-tc.expectedValQPS) > val/100 {
				t.Errorf("QPS after delayed Val() call: %f, Wanted: %f", val, tc.expectedBurstQPS)
			}
		})
	}
}

type promoteFromCandidate struct {
	hits int
}

func (p *promoteFromCandidate) ShouldPromote(key string, data []byte, stats promoter.Stats) bool {
	if p.hits > 0 {
		return true
	}
	p.hits++
	return false
}

// Ensures cache entries move properly through the stages of candidacy
// to full hotcache member. Simulates a galaxy where elements are always promoted,
// never promoted, etc
func TestPromotion(t *testing.T) {
	ctx := context.Background()
	testKey := "to-get"
	testCases := []struct {
		testName   string
		promoter   promoter.Interface
		cacheSize  int64
		checkCache func(ctx context.Context, t testing.TB, key string, val interface{}, okCand bool, okHot bool, tf *TestFetcher, g *Galaxy)
	}{
		{
			testName:  "never_promote",
			promoter:  promoter.Func(func(key string, data []byte, stats promoter.Stats) bool { return false }),
			cacheSize: 1 << 20,
			checkCache: func(_ context.Context, t testing.TB, _ string, _ interface{}, okCand bool, okHot bool, _ *TestFetcher, _ *Galaxy) {
				if !okCand {
					t.Error("Candidate not found in candidate cache")
				}
				if okHot {
					t.Error("Found candidate in hotcache")
				}
			},
		},
		{
			testName:  "always_promote",
			promoter:  promoter.Func(func(key string, data []byte, stats promoter.Stats) bool { return true }),
			cacheSize: 1 << 20,
			checkCache: func(_ context.Context, t testing.TB, _ string, val interface{}, _ bool, okHot bool, _ *TestFetcher, _ *Galaxy) {
				if !okHot {
					t.Error("Key not found in hotcache")
				} else if val == nil {
					t.Error("Found element in hotcache, but no associated data")
				}
			},
		},
		{
			testName:  "candidate_promotion",
			promoter:  &promoteFromCandidate{},
			cacheSize: 1 << 20,
			checkCache: func(ctx context.Context, t testing.TB, key string, _ interface{}, okCand bool, okHot bool, tf *TestFetcher, g *Galaxy) {
				if !okCand {
					t.Error("Candidate not found in candidate cache")
				}
				if okHot {
					t.Error("Found candidate in hotcache")
				}
				_, err := g.getFromPeer(ctx, tf, key)
				require.NoError(t, err)
				val, _ := g.hotCache.get(key)
				if string(val.(*valWithStat).data) != "got:"+testKey {
					t.Error("Did not promote from candidacy")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			fetcher := &TestFetcher{}
			testProto := &TestProtocol{}
			getter := func(_ context.Context, key string, dest Codec) error {
				return dest.UnmarshalBinary([]byte("got:"+key), time.Now().Add(5*time.Minute))
			}
			universe := NewUniverse(testProto, "promotion-test")
			galaxy := universe.NewGalaxy("test-galaxy", tc.cacheSize, GetterFunc(getter), WithPromoter(tc.promoter))
			_, err := galaxy.getFromPeer(ctx, fetcher, testKey)
			require.NoError(t, err)
			_, okCandidate := galaxy.candidateCache.get(testKey)
			value, okHot := galaxy.hotCache.get(testKey)
			tc.checkCache(ctx, t, testKey, value, okCandidate, okHot, fetcher, galaxy)

		})
	}

}

func TestRecorder(t *testing.T) {
	meter := view.NewMeter()
	meter.Start()
	defer meter.Stop()
	testView := &view.View{
		Measure:     MGets,
		TagKeys:     []tag.Key{GalaxyKey},
		Aggregation: view.Count(),
	}
	require.NoError(t, meter.Register(testView))

	getter := func(_ context.Context, key string, dest Codec) error {
		return dest.UnmarshalBinary([]byte("got:"+key), time.Now().Add(5*time.Minute))
	}
	u := NewUniverse(&TestProtocol{}, "test-universe", WithRecorder(meter))
	g := u.NewGalaxy("test", 1024, GetterFunc(getter))
	var s StringCodec
	err := g.Get(context.Background(), "foo", &s)
	if err != nil {
		t.Fatalf("error getting foo: %s", err)
	}

	rows, retErr := meter.RetrieveData(testView.Name)
	if retErr != nil {
		t.Fatalf("error getting data from view: %s", retErr)
	}

	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

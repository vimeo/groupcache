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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/vimeo/galaxycache/promoter"
	"github.com/vimeo/go-clocks/fake"
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
		return dest.UnmarshalBinary([]byte(str))
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
			resc <- string(s)
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
		stringGalaxy.Get(ctx, key, &res)
		bytesFlooded += int64(len(key) + len(res))
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

func (fetcher *TestFetcher) Fetch(ctx context.Context, galaxy string, key string) ([]byte, error) {
	if fetcher.fail {
		return nil, errors.New("simulated error from peer")
	}
	fetcher.hits++
	return []byte("got:" + key), nil
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

			universe.Set("fetcher0", "fetcher1", "fetcher2", "fetcher3")

			getter := func(_ context.Context, key string, dest Codec) error {
				// these are local hits
				testproto.TestFetchers["fetcher0"].hits++
				return dest.UnmarshalBinary([]byte("got:" + key))
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
				if string(got) != want {
					t.Errorf("%s: for key %q, got %q; want %q", tc.testName, key, got, want)
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
		return dest.UnmarshalBinary([]byte(testval))
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
			resc <- string(s)
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
	testKStats := keyStats{dQPS: windowedAvgQPS{}}
	testvws := g.newValWithStat([]byte(testval), &testKStats)
	wantBytes := int64(len(testkey)) + testvws.size()
	if g.mainCache.nbytes.Get() != wantBytes {
		t.Errorf("cache has %d bytes, want %d", g.mainCache.nbytes.Get(), wantBytes)
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
		name               string
		numGets            int
		numHeatBursts      int
		resetIdleAge       time.Duration
		burstInterval      time.Duration
		timeToVal          time.Duration
		expectedBurstQPS   float64
		expectedBurstCount int64
		expectedValQPS     float64
		expectedFinalCount int64
	}{
		{
			name:               "10k_heat_burst_1_sec",
			numGets:            10000,
			numHeatBursts:      5,
			resetIdleAge:       time.Minute,
			burstInterval:      1 * time.Second,
			timeToVal:          5 * time.Second,
			expectedBurstQPS:   10000.0,
			expectedBurstCount: 50000,
			expectedValQPS:     5000.0,
			expectedFinalCount: 50000,
		},
		{
			name:               "10k_heat_burst_5_secs",
			numGets:            10000,
			numHeatBursts:      5,
			resetIdleAge:       time.Minute,
			burstInterval:      5 * time.Second,
			timeToVal:          25 * time.Second,
			expectedBurstQPS:   2000.0,
			expectedBurstCount: 50000,
			expectedValQPS:     1000.0,
			expectedFinalCount: 50000,
		},
		{
			name:               "1k_heat_burst_4s_3s_reset_interval",
			numGets:            1000,
			numHeatBursts:      3,
			resetIdleAge:       time.Second * 3,
			burstInterval:      4 * time.Second,
			timeToVal:          5 * time.Second,
			expectedBurstQPS:   250.0, // only the last burst
			expectedBurstCount: 1000,
			expectedValQPS:     111.1,
			expectedFinalCount: 1000,
		},
	}

	for _, tc := range hcTests {
		t.Run(tc.name, func(t *testing.T) {
			u := NewUniverse(&TestProtocol{}, "test-universe")
			nowTime := time.Now()
			fc := fake.NewClock(nowTime)
			g := u.NewGalaxy("test-galaxy", 1<<20, GetterFunc(func(_ context.Context, key string, dest Codec) error {
				return dest.UnmarshalBinary([]byte("hello"))
			}), WithClock(fc), WithIdleStatsAgeResetWindow(tc.resetIdleAge))
			relNow := nowTime.Sub(g.baseTime)
			kStats := &keyStats{
				dQPS: windowedAvgQPS{trackEpoch: relNow},
			}
			value := g.newValWithStat([]byte("hello"), kStats)
			g.hotCache.add(keyToAdd, value)

			// blast the key in the hotcache with a bunch of hypothetical gets every few seconds
			for k := 0; k < tc.numHeatBursts; k++ {
				for ik := 0; ik < tc.numGets; ik++ {
					// force this key up to the top of the LRU
					g.hotCache.get(keyToAdd)
					kStats.dQPS.touch(g.resetIdleStatsAge, relNow)
				}
				t.Logf("QPS on %d gets in 1 second on burst #%d: %d\n", tc.numGets, k+1, kStats.dQPS.count)
				relNow += tc.burstInterval
				fc.Advance(tc.burstInterval)
			}
			cnt, val := kStats.dQPS.val(relNow)
			if math.Abs(val-tc.expectedBurstQPS) > val/100 { // ensure less than %1 error
				t.Errorf("QPS after bursts: %f, Wanted: %f", val, tc.expectedBurstQPS)
			}
			if cnt != tc.expectedBurstCount {
				t.Errorf("hit-count unexpected: %d; wanted %d", cnt, tc.expectedBurstCount)
			}
			value2 := g.newValWithStat([]byte("hello there"), nil)

			g.hotCache.add(keyToAdd+"2", value2) // ensure that hcStats are properly updated after adding
			g.maybeUpdateHotCacheStats()
			t.Logf("Hottest QPS: %f, Coldest QPS: %f\n", g.hcStatsWithTime.hcs.MostRecentQPS, g.hcStatsWithTime.hcs.LeastRecentQPS)

			relNow += tc.timeToVal
			fc.Advance(tc.timeToVal)
			cnt2, val2 := kStats.dQPS.val(relNow)
			if math.Abs(val2-tc.expectedValQPS) > val2/100 {
				t.Errorf("QPS after delayed Val() call; %s elapsed since val birth: %f, Wanted: %f",
					relNow-kStats.dQPS.trackEpoch, val2, tc.expectedBurstQPS)
			}
			if cnt2 != tc.expectedFinalCount {
				t.Errorf("hit-count unexpected: %d; wanted %d", cnt2, tc.expectedFinalCount)
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

type trackingNeverPromoter struct {
	stats []promoter.Stats
}

func (t *trackingNeverPromoter) ShouldPromote(key string, data []byte, stats promoter.Stats) bool {
	t.stats = append(t.stats, stats)
	return false
}

// Ensures cache entries move properly through the stages of candidacy
// to full hotcache member. Simulates a galaxy where elements are always promoted,
// never promoted, etc
func TestPromotion(t *testing.T) {
	outerCtx := context.Background()
	const testKey = "to-get"
	testCases := []struct {
		testName    string
		promoter    promoter.Interface
		cacheSize   int64
		firstCheck  func(ctx context.Context, t testing.TB, key string, val valWithStat, okCand bool, okHot bool, tf *TestFetcher, g *Galaxy)
		secondCheck func(ctx context.Context, t testing.TB, key string, val valWithStat, okCand bool, okHot bool, tf *TestFetcher, g *Galaxy)
	}{
		{
			testName:  "never_promote",
			promoter:  promoter.Func(func(key string, data []byte, stats promoter.Stats) bool { return false }),
			cacheSize: 1 << 20,
			firstCheck: func(_ context.Context, t testing.TB, _ string, _ valWithStat, okCand bool, okHot bool, _ *TestFetcher, _ *Galaxy) {
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
			firstCheck: func(_ context.Context, t testing.TB, _ string, val valWithStat, _ bool, okHot bool, _ *TestFetcher, _ *Galaxy) {
				if !okHot {
					t.Error("Key not found in hotcache")
				} else if val.data == nil {
					t.Error("Found element in hotcache, but no associated data")
				}
			},
		},
		{
			testName:  "candidate_promotion",
			promoter:  &promoteFromCandidate{},
			cacheSize: 1 << 20,
			firstCheck: func(ctx context.Context, t testing.TB, key string, _ valWithStat, okCand bool, okHot bool, tf *TestFetcher, g *Galaxy) {
				if !okCand {
					t.Error("Candidate not found in candidate cache")
				}
				if okHot {
					t.Error("Found candidate in hotcache")
				}
				g.getFromPeer(ctx, tf, key)
				val, okHot := g.hotCache.get(key)
				if !okHot {
					t.Errorf("key %q missing from hot cache", key)
				}
				if string(val.data) != "got:"+testKey {
					t.Error("Did not promote from candidacy")
				}
			},
		},
		{
			testName:  "never_promote_but_track",
			promoter:  &trackingNeverPromoter{},
			cacheSize: 1 << 20,
			firstCheck: func(_ context.Context, t testing.TB, _ string, _ valWithStat, okCand bool, okHot bool, _ *TestFetcher, g *Galaxy) {
				if okHot {
					t.Errorf("value unexpectedly in hot-cache")
				}
				if !okCand {
					t.Errorf("value unexpectedly missing from candidate-cache")
				}
				pro := g.opts.promoter.(*trackingNeverPromoter)
				if pro.stats == nil {
					t.Errorf("no stats recorded; promoter not called")
					return
				} else if len(pro.stats) != 1 {
					t.Errorf("incorrect call-count for promoter: %d; expected %d", len(pro.stats), 1)
					return
				}
				if pro.stats[0].Hits != 0 {
					t.Errorf("first hit had non-zero hits: %d", pro.stats[0].Hits)
				}
				if pro.stats[0].KeyQPS != 0 {
					t.Errorf("first hit had non-zero QPS: %f", pro.stats[0].KeyQPS)
				}
			},
			secondCheck: func(_ context.Context, t testing.TB, _ string, _ valWithStat, okCand bool, okHot bool, _ *TestFetcher, g *Galaxy) {
				if okHot {
					t.Errorf("value unexpectedly in hot-cache")
				}
				if !okCand {
					t.Errorf("value unexpectedly missing from candidate-cache")
				}
				pro := g.opts.promoter.(*trackingNeverPromoter)
				if pro.stats == nil {
					t.Errorf("no stats recorded; promoter not called")
					return
				} else if len(pro.stats) != 2 {
					t.Errorf("incorrect call-count for promoter: %d; expected %d", len(pro.stats), 2)
					return
				}
				if pro.stats[1].Hits != 1 {
					t.Errorf("second hit had unexpected hits: got %d; want %d", pro.stats[1].Hits, 1)
				}
				// one hit with near-zero-time passed == time will be bounded by the minimum time, which
				// is 100ms.
				if pro.stats[1].KeyQPS != 10.0 {
					t.Errorf("second hit had unexpected QPS: got %f; want %f", pro.stats[1].KeyQPS, 10.0)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(outerCtx)
			defer cancel()

			fetcher := &TestFetcher{}
			testProto := &TestProtocol{TestFetchers: map[string]*TestFetcher{"foobar": fetcher}}
			getter := func(_ context.Context, key string, dest Codec) error {
				return dest.UnmarshalBinary([]byte("got:" + key))
			}
			universe := NewUniverse(testProto, "promotion-test")
			universe.Set("foobar")
			galaxy := universe.NewGalaxy("test-galaxy", tc.cacheSize, GetterFunc(getter), WithPromoter(tc.promoter))
			sc := StringCodec("")
			{
				galaxy.Get(ctx, testKey, &sc)
				_, okCandidate := galaxy.candidateCache.get(testKey)
				value, okHot := galaxy.hotCache.get(testKey)
				tc.firstCheck(ctx, t, testKey, value, okCandidate, okHot, fetcher, galaxy)
			}
			if tc.secondCheck == nil {
				return
			}
			{
				galaxy.Get(ctx, testKey, &sc)
				_, okCandidate := galaxy.candidateCache.get(testKey)
				value, okHot := galaxy.hotCache.get(testKey)
				tc.secondCheck(ctx, t, testKey, value, okCandidate, okHot, fetcher, galaxy)
			}

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
	meter.Register(testView)

	getter := func(_ context.Context, key string, dest Codec) error {
		return dest.UnmarshalBinary([]byte("got:" + key))
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

func BenchmarkGetsSerialOneKey(b *testing.B) {
	b.ReportAllocs()

	ctx := context.Background()

	u := NewUniverse(&NullFetchProtocol{}, "test")

	const testKey = "somekey"
	const testVal = "testval"
	g := u.NewGalaxy("testgalaxy", 1024, GetterFunc(func(_ context.Context, key string, dest Codec) error {
		return dest.UnmarshalBinary([]byte(testVal))
	}))

	cd := ByteCodec{}
	b.ResetTimer()

	for z := 0; z < b.N; z++ {
		g.Get(ctx, testKey, &cd)
	}

}

func BenchmarkGetsSerialManyKeys(b *testing.B) {
	b.ReportAllocs()

	ctx := context.Background()

	u := NewUniverse(&NullFetchProtocol{}, "test")

	const testVal = "testval"
	g := u.NewGalaxy("testgalaxy", 1024, GetterFunc(func(_ context.Context, key string, dest Codec) error {
		return dest.UnmarshalBinary([]byte(testVal))
	}))

	cd := ByteCodec{}
	b.ResetTimer()

	for z := 0; z < b.N; z++ {
		k := "zzzz" + strconv.Itoa(z&0xffff)

		g.Get(ctx, k, &cd)
	}
}

func BenchmarkGetsParallelManyKeys(b *testing.B) {
	b.ReportAllocs()

	ctx := context.Background()

	u := NewUniverse(&NullFetchProtocol{}, "test")

	const testVal = "testval"
	g := u.NewGalaxy("testgalaxy", 1024, GetterFunc(func(_ context.Context, key string, dest Codec) error {
		return dest.UnmarshalBinary([]byte(testVal))
	}))

	cd := ByteCodec{}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for z := 0; pb.Next(); z++ {
			k := "zzzz" + strconv.Itoa(z&0xffff)

			g.Get(ctx, k, &cd)
		}
	})
}

func TestGetsParallelManyKeysWithGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	const N = 1 << 19
	// Traverse the powers of two
	for mul := 1; mul < 8; mul <<= 1 {
		t.Run(strconv.Itoa(mul), func(b *testing.T) {

			ctx := context.Background()

			u := NewUniverse(&NullFetchProtocol{}, "test")

			const testVal = "testval"
			g := u.NewGalaxy("testgalaxy", 1024, GetterFunc(func(_ context.Context, key string, dest Codec) error {
				return dest.UnmarshalBinary([]byte(testVal))
			}))

			gmp := runtime.GOMAXPROCS(-1)

			grs := gmp * mul

			iters := N / grs

			wg := sync.WaitGroup{}

			for gr := 0; gr < grs; gr++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					cd := ByteCodec{}
					for z := 0; z < iters; z++ {
						k := "zzzz" + strconv.Itoa(z&0x1fff)

						g.Get(ctx, k, &cd)
					}
				}()
			}
			wg.Wait()

		})
	}
}

func BenchmarkGetsParallelManyKeysWithGoroutines(b *testing.B) {
	// Traverse the powers of two
	for mul := 1; mul < 128; mul <<= 1 {
		b.Run(strconv.Itoa(mul), func(b *testing.B) {
			b.ReportAllocs()

			ctx := context.Background()

			u := NewUniverse(&NullFetchProtocol{}, "test")

			const testVal = "testval"
			g := u.NewGalaxy("testgalaxy", 1024, GetterFunc(func(_ context.Context, key string, dest Codec) error {
				return dest.UnmarshalBinary([]byte(testVal))
			}))

			gmp := runtime.GOMAXPROCS(-1)

			grs := gmp * mul

			iters := b.N / grs

			wg := sync.WaitGroup{}

			b.ResetTimer()

			for gr := 0; gr < grs; gr++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					cd := ByteCodec{}
					for z := 0; z < iters; z++ {
						k := "zzzz" + strconv.Itoa(z&0xffff)

						g.Get(ctx, k, &cd)
					}
				}()
			}
			wg.Wait()

		})
	}
}

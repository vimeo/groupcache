package galaxycache

import (
	"sync"
	"testing"
)

// TestPeers tests to ensure that an instance with given hash
// function results in the expected number of gets both locally and into each other peer
func TestPeersIncremental(t *testing.T) {

	hashOpts := &HashOptions{
		Replicas: 2,
		HashFn:   nil,
	}

	type addRemoveStep struct {
		add           []Peer
		remove        []string
		expectedPeers []Peer // don't include self (covered by includeSelf)
		parallel      bool   // step should be split up and run in parallel
		expectFailAdd bool
		expectFailRm  bool
		includeSelf   bool
		setIncSelf    bool
	}

	const selfID = "selfImpossibleFetcher"
	testCases := []struct {
		name        string
		initFunc    func(testProtocol *TestProtocol)
		cacheSize   int64
		includeSelf bool
		steps       []addRemoveStep
	}{
		{
			name:      "base_add_remove_serial",
			initFunc:  func(*TestProtocol) {},
			cacheSize: 1 << 20,
			steps: []addRemoveStep{
				{
					add:           []Peer{{ID: "fizzlebat3", URI: "fizzleboot3"}, {ID: "fizzlebat4", URI: "fizzleboot4"}},
					remove:        []string{},
					expectedPeers: []Peer{{ID: "fizzlebat3", URI: "fizzleboot3"}, {ID: "fizzlebat4", URI: "fizzleboot4"}},
					parallel:      false,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   true, // include self, but don't alter the default
					setIncSelf:    false,
				},
				{
					add:           []Peer{},
					remove:        []string{"fizzlebat3", "fizzleboat3"}, // remove a name that doesn't exist
					expectedPeers: []Peer{{ID: "fizzlebat4", URI: "fizzleboot4"}},
					parallel:      false,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   true, // include self, but don't alter the default
					setIncSelf:    false,
				},
				{
					add:           []Peer{},
					remove:        []string{}, // remove a name that doesn't exist
					expectedPeers: []Peer{{ID: "fizzlebat4", URI: "fizzleboot4"}},
					parallel:      false,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   true,
					setIncSelf:    true,
				},
				{
					add:           []Peer{},
					remove:        []string{}, // remove a name that doesn't exist
					expectedPeers: []Peer{{ID: "fizzlebat4", URI: "fizzleboot4"}},
					parallel:      false,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   false,
					setIncSelf:    true,
				},
				{
					add:           []Peer{},
					remove:        []string{}, // remove a name that doesn't exist
					expectedPeers: []Peer{{ID: "fizzlebat4", URI: "fizzleboot4"}},
					parallel:      false,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   true,
					setIncSelf:    true,
				},
			},
		},
		{
			name:      "base_add_parallel",
			initFunc:  func(*TestProtocol) {},
			cacheSize: 1 << 20,
			steps: []addRemoveStep{
				{
					add:           []Peer{{ID: "fizzlebat3", URI: "fizzleboot3"}, {ID: "fizzlebat4", URI: "fizzleboot4"}, {ID: "fizzlebat5", URI: "fizzleboot5"}},
					remove:        []string{},
					expectedPeers: []Peer{{ID: "fizzlebat3", URI: "fizzleboot3"}, {ID: "fizzlebat4", URI: "fizzleboot4"}, {ID: "fizzlebat5", URI: "fizzleboot5"}},
					parallel:      true,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   true, // include self, but don't alter the default
					setIncSelf:    false,
				},
				{
					add:           []Peer{},
					remove:        []string{"fizzlebat3", "fizzleboat3"}, // remove a name that doesn't exist
					expectedPeers: []Peer{{ID: "fizzlebat4", URI: "fizzleboot4"}, {ID: "fizzlebat5", URI: "fizzleboot5"}},
					parallel:      false,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   true, // include self, but don't alter the default
					setIncSelf:    false,
				},
			},
		},
		{
			name:      "one_peer_down",
			cacheSize: 1 << 20,
			initFunc: func(proto *TestProtocol) {
				proto.dialFails = map[string]struct{}{"fizzleboot3": {}}
			},
			steps: []addRemoveStep{
				{
					add:           []Peer{{ID: "fizzlebat3", URI: "fizzleboot3"}},
					remove:        []string{},
					expectedPeers: []Peer{},
					parallel:      false,
					expectFailAdd: true,
					expectFailRm:  false,
					includeSelf:   true, // include self, but don't alter the default
					setIncSelf:    false,
				},
				{
					add:           []Peer{{ID: "fizzlebat4", URI: "fizzleboot4"}},
					remove:        []string{},
					expectedPeers: []Peer{{ID: "fizzlebat4", URI: "fizzleboot4"}},
					parallel:      false,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   true, // include self, but don't alter the default
					setIncSelf:    false,
				},
				{
					add:           []Peer{},
					remove:        []string{"fizzlebat3", "fizzleboat3"}, // remove a name that doesn't exist
					expectedPeers: []Peer{{ID: "fizzlebat4", URI: "fizzleboot4"}},
					parallel:      false,
					expectFailAdd: false,
					expectFailRm:  false,
					includeSelf:   true, // include self, but don't alter the default
					setIncSelf:    false,
				},
			},
		},
	}

	for _, itbl := range testCases {
		tbl := itbl

		t.Run(tbl.name, func(t *testing.T) {
			t.Parallel()
			// instantiate test fetchers with the test protocol
			testproto := TestProtocol{
				TestFetchers: make(map[string]*TestFetcher),
			}

			checkErr := func(expErr bool, addErr error, opName string, stepIdx, opIdx int) {
				t.Helper()
				if expErr {
					if addErr == nil {
						t.Errorf("error expected at step %d (%dth %s) (got nil)",
							stepIdx, opIdx, opName)
					}
				} else if addErr != nil {
					t.Errorf("error %sing peer at step %d (%dth %s): %s",
						opName, stepIdx, stepIdx, opName, addErr)
				}
			}

			u := NewUniverseWithOpts(&testproto, selfID, hashOpts)

			tbl.initFunc(&testproto)

			for si, step := range tbl.steps {
				if step.setIncSelf {
					u.SetIncludeSelf(step.includeSelf)
				}
				if !step.parallel {
					for z, p := range step.add {
						addErr := u.AddPeer(p)
						// for now; assume that all adds for the step will fail if any of them
						// will
						checkErr(step.expectFailAdd, addErr, "add", z, si)
					}
					if len(step.remove) > 0 {
						removeErr := u.RemovePeers(step.remove...)
						checkErr(step.expectFailRm, removeErr, "remove", 0, si)
					}
				} else {
					// unbuffered channel that we'll close after all goroutines are spun up to
					// ensure they all run at roughly the same time
					gate := make(chan struct{})
					wg := sync.WaitGroup{}
					for iz, ip := range step.add {
						wg.Add(1)
						go func(i int, peer Peer) {
							defer wg.Done()
							<-gate
							addErr := u.AddPeer(peer)
							// for now; assume that all parallel adds for the step will fail
							// if any of them will
							checkErr(step.expectFailAdd, addErr, "add", i, si)
						}(iz, ip)
					}
					for iz, ip := range step.remove {
						wg.Add(1)
						go func(i int, peer string) {
							defer wg.Done()
							<-gate
							addErr := u.RemovePeers(peer)
							// for now; assume that all parallel adds for the step will fail
							// if any of them will
							checkErr(step.expectFailRm, addErr, "remove", i, si)
						}(iz, ip)
					}
					close(gate)
					wg.Wait()
				}

				allPeersSlice := u.peerPicker.peerIDs.GetReplicated("a", 10)
				allPeers := make(map[string]struct{}, len(allPeersSlice))
				for _, pn := range allPeersSlice {
					allPeers[pn] = struct{}{}
				}

				fetcherNames := make(map[string]struct{}, len(u.peerPicker.fetchers))
				fetcherURIs := make(map[string]struct{}, len(u.peerPicker.fetchers))
				for fn, f := range u.peerPicker.fetchers {
					fetcherNames[fn] = struct{}{}
					fetcherURIs[f.(*TestFetcher).uri] = struct{}{}
				}

				allFetchers := u.peerPicker.listPeers()
				allPeerIDs := make(map[string]struct{}, len(allFetchers))
				allFetcherURIs := make(map[string]struct{}, len(allFetchers))
				for peerID, fetcher := range allFetchers {
					allPeerIDs[peerID] = struct{}{}
					allFetcherURIs[fetcher.(*TestFetcher).uri] = struct{}{}
				}

				for _, expPeer := range step.expectedPeers {
					if _, ok := allPeers[expPeer.ID]; !ok {
						t.Errorf("missing peer %q from hashring at step %d", expPeer.ID, si)
					}
					delete(allPeers, expPeer.ID)
					if _, ok := fetcherNames[expPeer.ID]; !ok {
						t.Errorf("missing peer %q from fetchers map keys at step %d", expPeer.ID, si)
					}
					delete(fetcherNames, expPeer.ID)
					if _, ok := fetcherURIs[expPeer.URI]; !ok {
						t.Errorf("missing peer %q (URI %q) from fetchers values at step %d",
							expPeer.ID, expPeer.URI, si)
					}
					delete(fetcherURIs, expPeer.URI)
					// Checks for peer IDs and fetchers from listPeers method.
					if _, ok := allPeerIDs[expPeer.ID]; !ok {
						t.Errorf("missing peer %q from copy of fetchers map keys at step %d",
							expPeer.ID, si)
					}
					delete(allPeerIDs, expPeer.ID)
					if _, ok := allFetcherURIs[expPeer.URI]; !ok {
						t.Errorf("missing peer %q (URI %q) from copy of fetchers values at step %d",
							expPeer.ID, expPeer.URI, si)
					}
					delete(allFetcherURIs, expPeer.URI)
				}
				if step.includeSelf {
					if _, ok := allPeers[selfID]; !ok {
						t.Errorf("missing self entry in hashring at step %d", si)
					}
					delete(allPeers, selfID)
				}
				if len(allPeers) > 0 {
					t.Errorf("unexpected peer(s) in hashring at step %d: %v", si, allPeers)
				}
				if len(fetcherNames) > 0 {
					t.Errorf("unexpected peer(s) in fetcher-map at step %d: %v", si, fetcherNames)
				}
				if len(fetcherURIs) > 0 {
					t.Errorf("unexpected peer(s)' URI(s) in fetcher-map at step %d: %v", si, fetcherURIs)
				}
				// Checks for peer IDs and fetchers from listPeers method.
				if len(allPeerIDs) > 0 {
					t.Errorf("unexpected peer(s) in copy of fetcher-map at step %d: %v", si, allPeerIDs)
				}
				if len(allFetcherURIs) > 0 {
					t.Errorf("unexpected peer(s)' URI(s) in copy of fetcher-map at step %d: %v",
						si, allFetcherURIs)
				}
			}
		})
	}

}

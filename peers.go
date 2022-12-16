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

// peers.go defines how processes find and communicate with their peers.
// Each running Universe instance is a peer of each other, and it has
// authority over a set of keys within each galaxy (address space of data)
// -- which keys are handled by each peer is determined by the consistent
// hashing algorithm. Each instance fetches from another peer when it
// receives a request for a key for which that peer is the authority.

package galaxycache

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/vimeo/galaxycache/consistenthash"
)

const defaultReplicas = 50

// RemoteFetcher is the interface that must be implemented to fetch from
// other peers; the PeerPicker contains a map of these fetchers corresponding
// to each other peer address
type RemoteFetcher interface {
	Fetch(context context.Context, galaxy string, key string) ([]byte, error)
	// Close closes a client-side connection (may be a nop)
	Close() error
}

// PeerPicker is in charge of dealing with peers: it contains the hashing
// options (hash function and number of replicas), consistent hash map of
// peers, and a map of RemoteFetchers to those peers
type PeerPicker struct {
	fetchingProtocol FetchProtocol
	selfID           string
	includeSelf      bool
	peerIDs          *consistenthash.Map
	fetchers         map[string]RemoteFetcher // keyed by ID
	mapGen           peerSetGeneration
	mu               sync.RWMutex
	opts             HashOptions
}

// HashOptions specifies the the hash function and the number of replicas
// for consistent hashing
type HashOptions struct {
	// Replicas specifies the number of key replicas on the consistent hash.
	// If zero, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If nil, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash
}

// Creates a peer picker; called when creating a new Universe
func newPeerPicker(proto FetchProtocol, selfID string, options *HashOptions) *PeerPicker {
	pp := &PeerPicker{
		fetchingProtocol: proto,
		selfID:           selfID,
		fetchers:         make(map[string]RemoteFetcher),
		includeSelf:      true,
	}
	if options != nil {
		pp.opts = *options
	}
	if pp.opts.Replicas == 0 {
		pp.opts.Replicas = defaultReplicas
	}
	pp.peerIDs = consistenthash.New(pp.opts.Replicas, pp.opts.HashFn)
	return pp
}

// When passed a key, the consistent hash is used to determine which
// peer is responsible getting/caching it
func (pp *PeerPicker) pickPeer(key string) (RemoteFetcher, bool) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if URL := pp.peerIDs.Get(key); URL != "" && URL != pp.selfID {
		peer, ok := pp.fetchers[URL]
		return peer, ok
	}
	return nil, false
}

// setURLs assumes that peerURL == peerID (legacy reasons)
func (pp *PeerPicker) setURLs(peerURLs ...string) error {
	peers := make([]Peer, len(peerURLs))
	for i, url := range peerURLs {
		peers[i] = Peer{URI: url, ID: url}
	}
	return pp.set(peers...)
}

// Peer is an ID and ip:port/url tuple for a specific peer
type Peer struct {
	// Unique ID for this peer (e.g. in k8s may be a pod name)
	ID string
	// URI or URL that the registered PeerFetcher can connect to
	// URI should be a valid base URL,
	// for example "example.net:8000" or "10.32.54.231:8123".
	URI string
}

type peerSetGeneration uint64

type peerSetDiff struct {
	added        []Peer
	removed      map[string]struct{}
	selfIncluded bool
	generation   peerSetGeneration
}

func (pp *PeerPicker) diffAbsolutePeers(peers []Peer) peerSetDiff {
	pp.mu.RLock()
	defer pp.mu.RUnlock()
	currFetchers := make(map[string]struct{})

	for url := range pp.fetchers {
		currFetchers[url] = struct{}{}
	}

	selfIncluded := false
	newPeers := make([]Peer, 0, len(peers))
	for _, peer := range peers {
		if peer.ID == pp.selfID {
			selfIncluded = true

			continue
		}
		// open a new fetcher if there is currently no peer at url
		// also skip the self ID
		if _, ok := pp.fetchers[peer.ID]; !ok {
			newPeers = append(newPeers, peer)
			continue
		}
		delete(currFetchers, peer.ID)
	}

	return peerSetDiff{
		added:        newPeers,
		removed:      currFetchers,
		selfIncluded: selfIncluded,
		generation:   pp.mapGen,
	}
}

// if nil, false is returned, there's a version mismatch
// newFetchers should match indices in diff.added
func (pp *PeerPicker) updatePeers(diff peerSetDiff, newFetchers []RemoteFetcher) ([]RemoteFetcher, bool) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if diff.generation != pp.mapGen {
		return nil, false
	}
	pp.includeSelf = diff.selfIncluded
	// be optimistic: assume that we didn't race with anything. (we can do
	// some extra allocations in the uncommon/racy case)
	toClose := make([]RemoteFetcher, 0, len(diff.removed))
	for i, fetcher := range newFetchers {
		if _, ok := pp.fetchers[diff.added[i].ID]; ok {
			toClose = append(toClose, fetcher)
			continue
		}
		pp.fetchers[diff.added[i].ID] = fetcher
	}

	for remID := range diff.removed {
		if fetcher, ok := pp.fetchers[remID]; ok {
			toClose = append(toClose, fetcher)
		}
		delete(pp.fetchers, remID)
	}

	pp.regenerateHashringLocked()

	return toClose, true
}

func (pp *PeerPicker) set(peers ...Peer) error {
	loopPersistentFetchers := map[string]RemoteFetcher{}
	defer func() {
		for _, f := range loopPersistentFetchers {
			f.Close()
		}
	}()
	for {
		diff := pp.diffAbsolutePeers(peers)

		newfetchers := make([]RemoteFetcher, len(diff.added))
		dialEG := errgroup.Group{}
		for i, peerIter := range diff.added {
			if f, ok := loopPersistentFetchers[peerIter.ID]; ok {
				newfetchers[i] = f
				delete(loopPersistentFetchers, peerIter.ID)
				continue
			}
			peer := peerIter
			i := i
			dialEG.Go(func() error {
				newFetcher, err := pp.fetchingProtocol.NewFetcher(peer.URI)
				if err != nil {
					return err
				}
				newfetchers[i] = newFetcher
				return nil
			})
		}
		if dialErr := dialEG.Wait(); dialErr != nil {
			// NB: as of writing: we shouldn't get here in any real case as
			// neither the HTTP nor the gRPC RemoteFetcher implementations
			// actually do work when first constructed.
			for _, fetcher := range newfetchers {
				if fetcher == nil {
					continue
				}
				fetcher.Close()
			}
			return fmt.Errorf("failed to dial at least one backend: %w", dialErr)
		}

		// regenerate the hashring before we try to close any of the fetchers
		// so if they fail we don't end up with a hashring that's inconsistent
		// with the set of fetchers.
		rmFetchers, updated := pp.updatePeers(diff, newfetchers)
		if !updated {
			// Stash all the fetchers that we've already opened before looping
			for i, fetcher := range newfetchers {
				loopPersistentFetchers[diff.added[i].ID] = fetcher
			}
			continue
		}

		// if there's 0 or 1 to close, just iterate.
		// if there are more, we'll spin up goroutines and use an errgroup
		// (more for error-handling than efficiency)
		if len(rmFetchers) < 2 {
			for _, fetcher := range rmFetchers {
				err := fetcher.Close()
				if err != nil {
					return err
				}
			}
			return nil
		}
		closeEG := errgroup.Group{}
		for _, fetcher := range rmFetchers {
			f := fetcher
			closeEG.Go(func() error {
				if closeErr := f.Close(); closeErr != nil {
					return fmt.Errorf("failed to close RemoteFetcher: %w", closeErr)
				}
				return nil
			})
		}
		if closeErr := closeEG.Wait(); closeErr != nil {
			return fmt.Errorf("failed to close fetcher(s): %w", closeErr)
		}
		return nil
	}
}

func (pp *PeerPicker) checkPeerPresence(peer Peer) bool {
	pp.mu.RLock()
	defer pp.mu.RUnlock()
	_, ok := pp.fetchers[peer.ID]
	return ok
}

// returns whether the fetcher was inserted.
// if false is returned, the caller should close the fetcher.
func (pp *PeerPicker) insertPeer(peer Peer, fetcher RemoteFetcher) bool {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	_, ok := pp.fetchers[peer.ID]
	if ok {
		return false
	}

	pp.fetchers[peer.ID] = fetcher
	// No need to initialize a new peer hashring, we're only adding peers.
	pp.peerIDs.Add(peer.ID)
	return true
}

func (pp *PeerPicker) add(peer Peer) error {
	// Do a quick check to see if this peer is already there before we acquire the heavy write-lock
	if pp.checkPeerPresence(peer) {
		return nil
	}

	newFetcher, err := pp.fetchingProtocol.NewFetcher(peer.URI)
	if err != nil {
		return fmt.Errorf("fetcher init failed for %s (at %s): %w", peer.ID, peer.URI, err)
	}

	if !pp.insertPeer(peer, newFetcher) {
		// Something else raced and already added this fetcher
		// close it
		newFetcher.Close()
	}
	return nil
}

func (pp *PeerPicker) removePeers(peerIDs ...string) []RemoteFetcher {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return pp.removePeersLocked(peerIDs...)

}

func (pp *PeerPicker) removePeersLocked(peerIDs ...string) []RemoteFetcher {
	out := make([]RemoteFetcher, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		f, ok := pp.fetchers[peerID]
		if ok {
			out = append(out, f)
		}
		delete(pp.fetchers, peerID)
	}

	pp.regenerateHashringLocked()
	return out
}

func (pp *PeerPicker) regenerateHashringLocked() {
	selfAdj := 0
	if pp.includeSelf {
		selfAdj = 1
	}

	newPeerIDs := make([]string, selfAdj, len(pp.fetchers)+selfAdj)
	if pp.includeSelf {
		newPeerIDs[0] = pp.selfID
	}
	for id := range pp.fetchers {
		newPeerIDs = append(newPeerIDs, id)
	}

	// the consistenthash ring doesn't support removals so regenerate!
	pp.peerIDs = consistenthash.New(pp.opts.Replicas, pp.opts.HashFn)
	pp.peerIDs.Add(newPeerIDs...)
	pp.mapGen++
}

func (pp *PeerPicker) setIncludeSelf(inc bool) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.includeSelf = inc
	pp.regenerateHashringLocked()
}

func (pp *PeerPicker) remove(ids ...string) error {
	toClose := pp.removePeers(ids...)

	// if there's 0 or 1 to close, just iterate.
	// if there are more, we'll spin up goroutines and use an errgroup
	if len(toClose) < 2 {
		for _, f := range toClose {
			if closeErr := f.Close(); closeErr != nil {
				return closeErr
			}
		}
		return nil
	}
	eg := errgroup.Group{}
	for _, f := range toClose {
		f := f
		eg.Go(func() error {
			if closeErr := f.Close(); closeErr != nil {
				return fmt.Errorf("failed to close RemoteFetcher: %w", closeErr)
			}
			return nil
		})
	}

	return eg.Wait()
}

func (pp *PeerPicker) listPeers() map[string]RemoteFetcher {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	fetchers := pp.fetchers
	return fetchers
}

func (pp *PeerPicker) shutdown() error {
	pp.setIncludeSelf(false)
	// Clear out all the existing peers
	return pp.set()
}

// FetchProtocol defines the chosen fetching protocol to peers (namely
// HTTP or GRPC) and implements the instantiation method for that
// connection (creating a new RemoteFetcher)
type FetchProtocol interface {
	// NewFetcher instantiates the connection between the current and a
	// remote peer and returns a RemoteFetcher to be used for fetching
	// data from that peer
	NewFetcher(url string) (RemoteFetcher, error)
}

// NullFetchProtocol implements FetchProtocol, but always returns errors.
// (useful for unit-testing)
type NullFetchProtocol struct{}

// NewFetcher instantiates the connection between the current and a
// remote peer and returns a RemoteFetcher to be used for fetching
// data from that peer
func (n *NullFetchProtocol) NewFetcher(url string) (RemoteFetcher, error) {
	return &nullFetchFetcher{}, nil
}

type nullFetchFetcher struct{}

func (n *nullFetchFetcher) Fetch(context context.Context, galaxy string, key string) ([]byte, error) {
	return nil, errors.New("empty fetcher")
}

// Close closes a client-side connection (may be a nop)
func (n *nullFetchFetcher) Close() error {
	return nil
}

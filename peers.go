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

package galaxycache

import (
	"context"
	"sync"

	"github.com/vimeo/groupcache/consistenthash"
	pb "github.com/vimeo/groupcache/groupcachepb"
)

// RemoteFetcher is the interface that must be implemented to fetch from other peers; the PeerPicker contains a map of these fetchers corresponding to each peer address
type RemoteFetcher interface {
	Fetch(context context.Context, in *pb.GetRequest, out *pb.GetResponse) error
}

// PeerPicker is in charge of dealing with peers: it contains the hashing options (hash function and number of replicas), consistent hash map of peers, and a map of RemoteFetchers to those peers
type PeerPicker struct {
	fetchingProtocol FetchProtocol
	selfURL          string
	peers            *consistenthash.Map
	fetchers         map[string]RemoteFetcher
	mu               sync.RWMutex
	opts             HashOptions
}

// HashOptions specifies the the hash function and the number of replicas for consistent hashing
type HashOptions struct {
	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash
}

func newPeerPicker(proto FetchProtocol, self string, options *HashOptions) *PeerPicker {
	pp := &PeerPicker{
		fetchingProtocol: proto,
		selfURL:          self,
		fetchers:         make(map[string]RemoteFetcher),
	}
	if options != nil {
		pp.opts = *options
	}
	if pp.opts.Replicas == 0 {
		pp.opts.Replicas = defaultReplicas
	}
	pp.peers = consistenthash.New(pp.opts.Replicas, pp.opts.HashFn)
	return pp
}

func (pp *PeerPicker) pickPeer(key string) (RemoteFetcher, bool) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if pp.peers.IsEmpty() {
		return nil, false
	}
	if peer := pp.peers.Get(key); peer != pp.selfURL {
		return pp.fetchers[peer], true
	}
	return nil, false
}

func (pp *PeerPicker) set(peers ...string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.peers = consistenthash.New(pp.opts.Replicas, pp.opts.HashFn)
	pp.peers.Add(peers...)
	pp.fetchers = make(map[string]RemoteFetcher, len(peers))
	for _, peer := range peers {
		pp.fetchers[peer] = pp.fetchingProtocol.NewFetcher(peer)
	}
}

// FetchProtocol defines the chosen fetching protocol to peers (namely HTTP or GRPC) and implements the instantiation method for that connection (creating a new RemoteFetcher)
type FetchProtocol interface {
	// NewFetcher instantiates the connection between peers and returns a RemoteFetcher to be used for fetching from a peer
	NewFetcher(url string) RemoteFetcher
}

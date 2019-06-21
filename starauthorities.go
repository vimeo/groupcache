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

// starauthorities.go defines how processes (star authorities) find and communicate with their peers (other star authorities).
// Each running Universe instance has authority over a set of keys, or "stars", within each galaxy (address space of data) -- this star set is determined by the consistent hashing algorithm. Every one of these separate running Universe instances are therefore referred to as "star authorities". Each Universe fetches from other star authorities when it receives a request for a key that is handled by another instance.

package galaxycache

import (
	"context"
	"sync"

	"github.com/vimeo/groupcache/consistenthash"
	pb "github.com/vimeo/groupcache/groupcachepb"
)

// RemoteFetcher is the interface that must be implemented to fetch from other star authorities; the StarAuthorityPicker contains a map of these fetchers corresponding to each other star authority address
type RemoteFetcher interface {
	Fetch(context context.Context, in *pb.GetRequest, out *pb.GetResponse) error
}

// StarAuthorityPicker is in charge of dealing with star authorities: it contains the hashing options (hash function and number of replicas), consistent hash map of star authorities, and a map of RemoteFetchers to those star authorities
type StarAuthorityPicker struct {
	fetchingProtocol FetchProtocol
	selfURL          string
	starAuthorities  *consistenthash.Map
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

// Creates a star authority picker; called when creating a new Universe
func newStarAuthorityPicker(proto FetchProtocol, selfURL string, options *HashOptions) *StarAuthorityPicker {
	pp := &StarAuthorityPicker{
		fetchingProtocol: proto,
		selfURL:          selfURL,
		fetchers:         make(map[string]RemoteFetcher),
	}
	if options != nil {
		pp.opts = *options
	}
	if pp.opts.Replicas == 0 {
		pp.opts.Replicas = defaultReplicas
	}
	pp.starAuthorities = consistenthash.New(pp.opts.Replicas, pp.opts.HashFn)
	return pp
}

// When passed a key ("star"), the consistent hash is used to determine which star authority is responsible getting/caching it
func (pp *StarAuthorityPicker) pickStarAuthority(key string) (RemoteFetcher, bool) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if pp.starAuthorities.IsEmpty() {
		return nil, false
	}
	if URL := pp.starAuthorities.Get(key); URL != pp.selfURL {
		return pp.fetchers[URL], true
	}
	return nil, false
}

func (pp *StarAuthorityPicker) set(starAuthorityURLs ...string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.starAuthorities = consistenthash.New(pp.opts.Replicas, pp.opts.HashFn)
	pp.starAuthorities.Add(starAuthorityURLs...)
	pp.fetchers = make(map[string]RemoteFetcher, len(starAuthorityURLs))
	for _, URL := range starAuthorityURLs {
		pp.fetchers[URL] = pp.fetchingProtocol.NewFetcher(URL)
	}
}

// FetchProtocol defines the chosen fetching protocol to star authorities (namely HTTP or GRPC) and implements the instantiation method for that connection (creating a new RemoteFetcher)
type FetchProtocol interface {
	// NewFetcher instantiates the connection between the current and a remote star authority and returns a RemoteFetcher to be used for fetching data from that authority
	NewFetcher(url string) RemoteFetcher
}

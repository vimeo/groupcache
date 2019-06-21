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

// RemoteFetcher is the interface that must be implemented to fetch from other arms; the SpiralArmPicker contains a map of these fetchers corresponding to each arm address
type RemoteFetcher interface {
	Fetch(context context.Context, in *pb.GetRequest, out *pb.GetResponse) error
}

// SpiralArmPicker is in charge of dealing with arms: it contains the hashing options (hash function and number of replicas), consistent hash map of arms, and a map of RemoteFetchers to those arms
type SpiralArmPicker struct {
	fetchingProtocol FetchProtocol
	selfURL          string
	arms            *consistenthash.Map
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

func newSpiralArmPicker(proto FetchProtocol, self string, options *HashOptions) *SpiralArmPicker {
	pp := &SpiralArmPicker{
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
	pp.arms = consistenthash.New(pp.opts.Replicas, pp.opts.HashFn)
	return pp
}

func (pp *SpiralArmPicker) pickSpiralArm(key string) (RemoteFetcher, bool) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if pp.arms.IsEmpty() {
		return nil, false
	}
	if arm := pp.arms.Get(key); arm != pp.selfURL {
		return pp.fetchers[arm], true
	}
	return nil, false
}

func (pp *SpiralArmPicker) set(arms ...string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.arms = consistenthash.New(pp.opts.Replicas, pp.opts.HashFn)
	pp.arms.Add(arms...)
	pp.fetchers = make(map[string]RemoteFetcher, len(arms))
	for _, arm := range arms {
		pp.fetchers[arm] = pp.fetchingProtocol.NewFetcher(arm)
	}
}

// FetchProtocol defines the chosen fetching protocol to arms (namely HTTP or GRPC) and implements the instantiation method for that connection (creating a new RemoteFetcher)
type FetchProtocol interface {
	// NewFetcher instantiates the connection between arms and returns a RemoteFetcher to be used for fetching from a arm
	NewFetcher(url string) RemoteFetcher
}

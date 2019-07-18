/*
 Copyright 2019 Vimeo Inc.

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

package grpcpeerconn

import (
	"context"
	"fmt"

	gc "github.com/vimeo/galaxycache"
	pb "github.com/vimeo/galaxycache/galaxycachepb"

	"google.golang.org/grpc"
)

// GRPCFetchProtocol specifies GRPC specific options for
// GRPC-based peer communcation
type GRPCFetchProtocol struct {
	// connection set up configurations for all peers
	PeerDialOptions []grpc.DialOption
}

type grpcFetcher struct {
	address string
	conn    *grpc.ClientConn
	client  pb.GalaxyCacheClient
}

// NewGRPCFetchProtocol creates a fetch-protocol implementation
// using GRPC for communicating with peers. Users without TLS
// certificates on the peers operating as servers should specify
// grpc.WithInsecure() as one of the arguments.
func NewGRPCFetchProtocol(dialOpts ...grpc.DialOption) *GRPCFetchProtocol {
	return &GRPCFetchProtocol{PeerDialOptions: dialOpts}
}

// NewFetcher implements the FetchProtocol interface for
// GRPCFetchProtocol by constructing a new fetcher to fetch
// from peers via GRPC
func (gp *GRPCFetchProtocol) NewFetcher(address string) (gc.RemoteFetcher, error) {
	conn, err := grpc.Dial(address, gp.PeerDialOptions...)
	if err != nil {
		return nil, err
	}
	client := pb.NewGalaxyCacheClient(conn)
	return &grpcFetcher{address: address, conn: conn, client: client}, nil
}

// Fetch here implements the RemoteFetcher interface for
// sending Gets to peers over an RPC connection
func (g *grpcFetcher) Fetch(ctx context.Context, galaxy string, key string) ([]byte, error) {
	resp, err := g.client.GetFromPeer(ctx, &pb.GetRequest{
		Galaxy: galaxy,
		Key:    key})
	if err != nil {
		return nil, fmt.Errorf("Failed to fetch from peer over RPC [%q, %q]: %s", galaxy, g.address, err)
	}

	return resp.Value, nil
}

// Close here implements the RemoteFetcher interface for
// closing a client-side RPC connection opened by the fetcher
func (g *grpcFetcher) Close() error {
	if g.conn == nil {
		return nil
	}
	return g.conn.Close()
}

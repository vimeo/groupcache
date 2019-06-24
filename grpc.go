/*
 Copyright 2019 Will Greenberg
 Adapted from https://github.com/charithe/gcgrpcpool/blob/master/gcgrpcpool.go

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

package galaxycache

import (
	"context"
	"fmt"

	pb "github.com/vimeo/groupcache/groupcachepb"
	"google.golang.org/grpc"
)

// // GOOD
// type GRPCPool struct {
// 	self string
// 	opts GRPCPoolOptions
// 	mu   sync.Mutex
// 	// peers       *consistenthash.Map
// 	grpcGetters map[string]*grpcFetcher
// }

// GRPCFetchProtocol specifies GRPC specific options for GRPC-based star authority communcation
type GRPCFetchProtocol struct {
	// connection set up configurations for all peers
	PeerDialOptions []grpc.DialOption
}

type GRPCOptions struct {
	// connection set up configurations for all peers
	PeerDialOptions []grpc.DialOption
	// if true, there will be no TLS
	// AllInsecureConnections bool
}

type grpcFetcher struct {
	address string
	conn    *grpc.ClientConn
}

// NewGRPCFetchProtocol creates an HTTP fetch protocol to be passed into a Universe constructor
func NewGRPCFetchProtocol(opts *GRPCOptions) *GRPCFetchProtocol {
	newProto := &GRPCFetchProtocol{
		PeerDialOptions: []grpc.DialOption{grpc.WithInsecure()},
	}

	if opts == nil {
		return newProto
	}
	if len(opts.PeerDialOptions) > 0 {
		newProto.PeerDialOptions = opts.PeerDialOptions
	}
	return newProto
}

// NewFetcher implements the FetchProtocol interface for GRPCFetchProtocol by constructing a new fetcher to fetch from peers ("star authorities") via GRPC
func (gp *GRPCFetchProtocol) NewFetcher(address string) RemoteFetcher {
	conn, err := grpc.Dial(address, gp.PeerDialOptions...)
	if err != nil {
		fmt.Printf("Failure connecting: [%v]\n", err) // TODO: remove print
		return nil
	}
	fmt.Printf("Success connecting to new Getter at [%s]\n", address) // TODO: remove print
	return &grpcFetcher{address: address, conn: conn}
}

type GRPCHandler struct {
	parentUniverse *Universe
}

// RegisterGRPCServer creates a new GRPC server and GRPCHandler and registers them for RPC use;
// the purpose of the GRPCHandler is to keep track of the parent universe in order to access the groups for Get calls
func RegisterGRPCServer(universe *Universe, grpcServer *grpc.Server) {
	pb.RegisterGalaxycacheServer(grpcServer, &GRPCHandler{parentUniverse: universe})
}

// GetFromRemote implements the generated pb.GalaxycacheServer interface, making an internal Get() after receiving a remote call
func (gp *GRPCHandler) GetFromRemote(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	group := gp.parentUniverse.GetGalaxy(req.Galaxy)
	if group == nil {
		// log.Warnf("Unable to find group [%s]", req.Group)
		return nil, fmt.Errorf("Unable to find group [%s]", req.Galaxy)
	}

	group.Stats.ServerRequests.Add(1) // keep track of the num of req... was a TODO to remove this?

	var value []byte
	err := group.Get(ctx, req.Key, AllocatingByteSliceSink(&value))
	if err != nil {
		// log.WithError(err).Warnf("Failed to retrieve [%s]", req)
		return nil, fmt.Errorf("Failed to retrieve [%s]: %v", req, err)
	}

	return &pb.GetResponse{Value: value}, nil
}

// Fetch here implements the RemoteFetcher interface for sending Gets to peers over an RPC connection
func (g *grpcFetcher) Fetch(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	client := pb.NewGalaxycacheClient(g.conn)
	resp, err := client.GetFromRemote(context.Background(), &pb.GetRequest{
		Galaxy: in.Galaxy,
		Key:    in.Key}) // passed with an empty Context
	if err != nil {
		return fmt.Errorf("Failed to GET [%s]: %v", in, err)
	}

	out.Value = resp.Value
	return nil
}

func (g *grpcFetcher) close() {
	if g.conn != nil {
		g.conn.Close()
	}
}

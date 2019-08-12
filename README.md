# galaxycache

galaxycache is a caching and cache-filling library, adapted from groupcache, intended as a
replacement for memcached in many cases.

For API docs and examples, see http://godoc.org/github.com/vimeo/galaxycache

## Quick Start

### Initializing a peer
```go
// Generate the protocol for this peer to Fetch from others with (package includes HTTP and gRPC)
grpcProto := NewGRPCFetchProtocol(grpc.WithInsecure())

// HTTP protocol as an alternative (passing the nil argument ensures use of the default basepath
// and opencensus Transport as an http.RoundTripper)
httpProto := NewHTTPFetchProtocol(nil)

// Create a new Universe with the chosen peer connection protocol and the URL of this process
u := NewUniverse(grpcProto, "my-url")

// Set the Universe's list of peer addresses for the distributed cache
u.Set("peer1-url", "peer2-url", "peer3-url")

// Define a BackendGetter (here as a function) for retrieving data
getter := GetterFunc(func(_ context.Context, key string, dest Codec) error {
   // Define your method for retrieving non-cached data here, i.e. from a database
})

// Create a new Galaxy within the Universe with a name, the max capacity of cache space you would
// like to allocate, and your BackendGetter. The hotcache defaults to 1/8 of the total cache size,
// the Promoter defaults to a simple QPS comparison, and the maximum number of hotcache candidates
// defaults to 100; these can be overwritten with variadic options (WithHotCacheRatio() used below
// as an example to make the hotcache 1/4 the total cache size)
g := u.NewGalaxy("galaxy-1", 1 << 20, getter, WithHotCacheRatio(4))

// In order to receive Fetch requests from peers over HTTP or gRPC, we must register this universe
// to handle those requests

// gRPC Server registration (note: you must create the server with an ocgrpc.ServerHandler for
// opencensus metrics to propogate properly)
grpcServer := grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{}))
RegisterGRPCServer(u, grpcServer)

// HTTP Handler registration (passing nil for the second argument will ensure use of the default 
// basepath, passing nil for the third argument will ensure use of the DefaultServeMux wrapped 
// by opencensus)
RegisterHTTPHandler(u, nil, nil)

// Refer to the http/grpc godocs for information on how to serve using the registered HTTP handler
// or gRPC server, respectively

```
### Getting a value
```go
// Create a Codec for unmarshaling data into your format of choice - the package includes 
// implementations for []byte and string formats, and the protocodec subpackage includes the 
// protobuf format
sCodec := StringCodec{}

// Call Get on the Galaxy to retrieve data and unmarshal it into your Codec
ctx := context.Background()
err := g.Get(ctx, "my-key", sCodec)
if err != nil {
   // handle if Get returns an error
}

// Shutdown all open connections between peers before killing the process
u.Shutdown()

```

## Changes from groupcache

Our changes include the following:
* Overhauled API to improve useability and configurability
* Improvements to testing by removing global state
* Improvement to connection efficiency between peers with the addition of gRPC
* Added a `Promoter` interface for choosing which keys get hotcached
* Made some core functionality more generic (e.g. replaced the `Sink` object with a `Codec` marshaler interface, removed `Byteview`)

We also changed the naming scheme of objects and methods to clarify their purpose with the help of a space-themed scenario:

Each process within a set of peer processes contains a `Universe` which encapsulates a map of `Galaxies` (previously called `Groups`). Each `Universe` contains the same set of `Galaxies`, but each `key` (think of it as a "star") has a single associated authoritative peer (determined by the consistent hash function). 

When `Get` is called for a key in a `Galaxy` in some process called Process-A:
1. The local cache (both maincache and hotcache) in Process-A is checked first
2. On a cache miss, the `PeerPicker` object delegates to the peer authoritative over the requested key
3. Depends on which peer is authoritative over this key...
If the Process_A is the authority:
   - Process_A uses its `BackendGetter` to get the data, and populates its local maincache
If Process_A is _not_ the authority:
   - Process_A calls `Fetch` on the authoritative remote peer, Process_B
   - Process_B then performs a `Get` to either find the data from its own local cache or use the specified `BackendGetter` to get the data from elsewhere, such as by querying a database
   - Process_B populates its maincache with the data before serving it back to Process_A
   - Process_A determines whether the key is hot enough to promote to the hotcache
      - If it is, then the hotcache for Process_A is populated with the key/data
4. The data is unmarshaled into the `Codec` passed into `Get`


### New architecture and API

* Renamed `Group` type to `Galaxy`, `Getter` to `BackendGetter`, `Get` to `Fetch` (for newly named `RemoteFetcher` interface, previously called `ProtoGetter`)
* Reworked `PeerPicker` interface into a struct; contains a `FetchProtocol` and `RemoteFetchers` (generalizing for HTTP and GRPC fetching implementations), a hash map of other peer addresses, and a self URL

### No more global state

* Removed all global variables to allow for multithreaded testing by implementing a `Universe` container that holds the `Galaxies` (previously a global `groups` map) and `PeerPicker` (part of what used to be `HTTPPool`)
* Added methods to `Universe` to allow for simpler handling of most galaxycache operations (setting Peers, instantiating a Picker, etc)

### New structure for fetching from peers (with gRPC support)

* Added an `HTTPHandler` and associated registration function for serving HTTP requests by reaching into an associated `Universe` (deals with the other function of the deprecated `HTTPPool`)
* Reworked tests to fit new architecture
* Renamed files to match new type names

### A smarter Hotcache with configurable promotion logic

* Promoter package provides an interface for creating your own `ShouldPromote` method to determine whether a key should be added to the hotcache
* Newly added Candidate Cache keeps track of peer-owned keys (without associated data) that have not yet been promoted to the hotcache
* Provided variadic options for `Galaxy` construction to override default promotion logic (with your promoter, max number of candidates, and relative hotcache size to maincache)


## Comparison to memcached

See: https://github.com/golang/groupcache/blob/master/README.md

## Loading process

In a nutshell, a galaxycache lookup of **Get("foo")** looks like:

(On machine #5 of a set of N machines running the same code)

 1. Is the value of "foo" in local memory because it's super hot?  If so, use it.

 2. Is the value of "foo" in local memory because peer #5 (the current
    peer) is the owner of it?  If so, use it.

 3. Amongst all the peers in my set of N, am I the owner of the key
    "foo"?  (e.g. does it consistent hash to 5?)  If so, load it.  If
    other callers come in, via the same process or via RPC requests
    from peers, they block waiting for the load to finish and get the
    same answer.  If not, RPC to the peer that's the owner and get
    the answer.  If the RPC fails, just load it locally (still with
    local dup suppression).

## Help

Use the golang-nuts mailing list for any discussion or questions.

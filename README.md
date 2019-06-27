# galaxycache

galaxycache is a caching and cache-filling library, adapted from groupcache, intended as a
replacement for memcached in many cases.

For API docs and examples, see http://godoc.org/github.com/vimeo/galaxycache

## Summary of changes

### New architecture and naming scheme

* Renamed Group type to Galaxy, Getter to BackendGetter, Get to Fetch (for newly named RemoteFetcher interface, previously called ProtoGetter)
* Reworked PeerPicker interface into a struct; contains a FetchProtocol and RemoteFetchers (generalizing for HTTP and GRPC fetching implementations), a hash map of other peer addresses, and a self URL

### No more global state

* Removed all global variables to allow for multithreaded testing by implementing a Universe container that holds the Galaxies (previously a global "groups" map) and PeerPicker (part of what used to be HTTPPool)
* Added methods to Universe to allow for simpler handling of most galaxycache operations through the Universe (setting Peers, instantiating a Picker, etc)

### New structure for fetching from peers (with **gRPC support**!)
* Added an HTTPHandler and associated registration function for serving HTTP requests by reaching into an associated Universe (deals with the other function of the deprecated HTTPPool)
* Reworked tests to fit new architecture
* Renamed files to match new type names

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

<<<<<<< HEAD
## Presentations

See http://talks.golang.org/2013/oscon-dl.slide

=======
>>>>>>> Rewrite README to include galaxycache-related information (draft 1)
## Help

Use the golang-nuts mailing list for any discussion or questions.

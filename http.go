/*
Copyright 2013 Google Inc.

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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	pb "github.com/vimeo/groupcache/groupcachepb"

	"go.opencensus.io/stats"
)

const defaultBasePath = "/_galaxycache/"

const defaultReplicas = 50

// HTTPFetchProtocol specifies HTTP specific options for HTTP-based star authority communication
type HTTPFetchProtocol struct {
	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	Transport func(context.Context) http.RoundTripper
	BasePath  string
}

// HTTPOptions specifies a base path for serving and fetching.
// *ONLY SPECIFY IF NOT USING THE DEFAULT "/_galaxycache/" BASE PATH*.
type HTTPOptions struct {
	Transport func(context.Context) http.RoundTripper
	basePath  string
}

// NewHTTPFetchProtocol creates an HTTP fetch protocol to be passed into a Universe constructor;
// uses a user chosen base path specified in HTTPOptions (or the default "/_galaxycache/" base path if passed nil).
// *You must use the same base path for the HTTPFetchProtocol and the HTTPHandler on the same Universe*.
func NewHTTPFetchProtocol(opts *HTTPOptions) *HTTPFetchProtocol {
	newProto := &HTTPFetchProtocol{
		BasePath: defaultBasePath,
	}
	if opts == nil {
		return newProto
	}
	if opts.basePath != "" {
		newProto.BasePath = opts.basePath
	}
	if opts.Transport != nil {
		newProto.Transport = opts.Transport
	}
	return newProto
}

// NewFetcher implements the Protocol interface for HTTPProtocol by constructing a new fetcher to fetch from starAuthorities via HTTP
func (hp *HTTPFetchProtocol) NewFetcher(url string) RemoteFetcher {
	return &httpFetcher{transport: hp.Transport, baseURL: url + hp.BasePath}
}

// HTTPHandler implements the HTTP handler necessary to serve an HTTP request; it contains a pointer to its parent Universe in order to access its Galaxys
type HTTPHandler struct {
	parentUniverse *Universe
	basePath       string
}

// RegisterHTTPHandler sets up an HTTPHandler with a user specified path and serveMux (if non nil) to handle requests to the given Universe. If both opts and serveMux are nil, defaultBasePath and DefaultServeMux will be used.
// *You must use the same base path for the HTTPFetchProtocol and the HTTPHandler on the same Universe*.
func RegisterHTTPHandler(universe *Universe, opts *HTTPOptions, serveMux *http.ServeMux) {
	basePath := defaultBasePath
	if opts != nil {
		basePath = opts.basePath
	}
	newHTTPHandler := &HTTPHandler{basePath: basePath, parentUniverse: universe}
	if serveMux == nil {
		http.Handle(basePath, newHTTPHandler)
	} else {
		serveMux.Handle(basePath, newHTTPHandler)
	}
}

func (handler *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	// fmt.Println("Serving request!")
	if !strings.HasPrefix(r.URL.Path, handler.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	parts := strings.SplitN(r.URL.Path[len(handler.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	galaxyName := parts[0]
	key := parts[1]

	// Fetch the value for this galaxy/key.
	galaxy := handler.parentUniverse.GetGalaxy(galaxyName)
	if galaxy == nil {
		http.Error(w, "no such galaxy: "+galaxyName, http.StatusNotFound)
		return
	}

	ctx := r.Context()

	// TODO: remove galaxy.Stats from here
	galaxy.Stats.ServerRequests.Add(1)
	stats.Record(ctx, MServerRequests.M(1))
	var value []byte
	err := galaxy.Get(ctx, key, AllocatingByteSliceSink(&value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.GetResponse{Value: value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(body)
}

type httpFetcher struct {
	transport func(context.Context) http.RoundTripper
	baseURL   string
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func (h *httpFetcher) Fetch(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGalaxy()),
		url.QueryEscape(in.GetKey()),
	)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	tr := http.DefaultTransport
	if h.transport != nil {
		tr = h.transport(ctx)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

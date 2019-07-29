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
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"go.opencensus.io/stats"
)

const defaultBasePath = "/_galaxycache/"

// HTTPFetchProtocol specifies HTTP specific options for HTTP-based
// peer communication
type HTTPFetchProtocol struct {
	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	transport func(context.Context) http.RoundTripper
	basePath  string
}

// HTTPOptions specifies a base path for serving and fetching.
// *ONLY SPECIFY IF NOT USING THE DEFAULT "/_galaxycache/" BASE PATH*.
type HTTPOptions struct {
	Transport func(context.Context) http.RoundTripper
	BasePath  string
}

// NewHTTPFetchProtocol creates an HTTP fetch protocol to be passed
// into a Universe constructor; uses a user chosen base path specified
// in HTTPOptions (or the default "/_galaxycache/" base path if passed nil).
// *You must use the same base path for the HTTPFetchProtocol and the
// HTTPHandler on the same Universe*.
func NewHTTPFetchProtocol(opts *HTTPOptions) *HTTPFetchProtocol {
	newProto := &HTTPFetchProtocol{
		basePath: defaultBasePath,
	}
	if opts == nil {
		return newProto
	}
	if opts.BasePath != "" {
		newProto.basePath = opts.BasePath
	}
	if opts.Transport != nil {
		newProto.transport = opts.Transport
	}
	return newProto
}

// NewFetcher implements the Protocol interface for HTTPProtocol by constructing
// a new fetcher to fetch from peers via HTTP
func (hp *HTTPFetchProtocol) NewFetcher(url string) (RemoteFetcher, error) {
	return &httpFetcher{transport: hp.transport, baseURL: url + hp.basePath}, nil
}

// HTTPHandler implements the HTTP handler necessary to serve an HTTP
// request; it contains a pointer to its parent Universe in order to access
// its galaxies
type HTTPHandler struct {
	universe *Universe
	basePath string
}

// RegisterHTTPHandler sets up an HTTPHandler with a user specified path
// and serveMux (if non nil) to handle requests to the given Universe.
// If both opts and serveMux are nil, defaultBasePath and DefaultServeMux
// will be used. *You must use the same base path for the HTTPFetchProtocol
// and the HTTPHandler on the same Universe*.
func RegisterHTTPHandler(universe *Universe, opts *HTTPOptions, serveMux *http.ServeMux) {
	basePath := defaultBasePath
	if opts != nil {
		basePath = opts.BasePath
	}
	newHTTPHandler := &HTTPHandler{basePath: basePath, universe: universe}
	if serveMux == nil {
		http.Handle(basePath, newHTTPHandler)
	} else {
		serveMux.Handle(basePath, newHTTPHandler)
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	if !strings.HasPrefix(r.URL.Path, h.basePath) {
		panic("HTTPHandler serving unexpected path: " + r.URL.Path)
	}
	parts := strings.SplitN(r.URL.Path[len(h.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	galaxyName := parts[0]
	key := parts[1]

	// Fetch the value for this galaxy/key.
	galaxy := h.universe.GetGalaxy(galaxyName)
	if galaxy == nil {
		http.Error(w, "no such galaxy: "+galaxyName, http.StatusNotFound)
		return
	}

	ctx := r.Context()

	// TODO: remove galaxy.Stats from here
	galaxy.Stats.ServerRequests.Add(1)
	stats.Record(ctx, MServerRequests.M(1))
	var value ByteCodec
	err := galaxy.Get(ctx, key, &value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(value)
}

type httpFetcher struct {
	transport func(context.Context) http.RoundTripper
	baseURL   string
}

// Fetch here implements the RemoteFetcher interface for sending a GET request over HTTP to a peer
func (h *httpFetcher) Fetch(ctx context.Context, galaxy string, key string) ([]byte, error) {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(galaxy),
		url.QueryEscape(key),
	)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport
	if h.transport != nil {
		tr = h.transport(ctx)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned HTTP response status code: %v", res.Status)
	}
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}
	return data, nil
}

// Close here implements the RemoteFetcher interface for closing (does nothing for HTTP)
func (h *httpFetcher) Close() error {
	return nil
}

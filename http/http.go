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

package http

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vimeo/galaxycache"
	gc "github.com/vimeo/galaxycache"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats"
)

const defaultBasePath = "/_galaxycache/"

// When the retrieved value should expire. Unix timestamp in milliseconds.
const ttlHeader = "X-Galaxycache-Expire"

// HTTPFetchProtocol specifies HTTP specific options for HTTP-based
// peer communication
type HTTPFetchProtocol struct {
	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	transport http.RoundTripper
	basePath  string
}

// HTTPOptions can specify the transport, base path, and stats.Recorder for
// serving and fetching. *ONLY SPECIFY IF NOT USING THE DEFAULT "/_galaxycache/"
// BASE PATH*.
type HTTPOptions struct {
	Transport http.RoundTripper
	BasePath  string
	Recorder  stats.Recorder
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
		newProto.transport = &ochttp.Transport{}
		return newProto
	}
	newProto.transport = &ochttp.Transport{
		Base: opts.Transport,
	}
	if opts.BasePath != "" {
		newProto.basePath = opts.BasePath
	}

	return newProto
}

// NewFetcher implements the Protocol interface for HTTPProtocol by constructing
// a new fetcher to fetch from peers via HTTP
// Prefixes URL with http:// if neither http:// nor https:// are prefixes of
// the URL argument.
func (hp *HTTPFetchProtocol) NewFetcher(url string) (gc.RemoteFetcher, error) {
	if !(strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://")) {
		url = "http://" + url
	}
	return &httpFetcher{transport: hp.transport, baseURL: url + hp.basePath}, nil
}

// HTTPHandler implements the HTTP handler necessary to serve an HTTP
// request; it contains a pointer to its parent Universe in order to access
// its galaxies
type HTTPHandler struct {
	universe *gc.Universe
	basePath string
	recorder stats.Recorder
}

// RegisterHTTPHandler sets up an HTTPHandler with a user specified path
// and serveMux (if non nil) to handle requests to the given Universe.
// If both opts and serveMux are nil, defaultBasePath and DefaultServeMux
// will be used. *You must use the same base path for the HTTPFetchProtocol
// and the HTTPHandler on the same Universe*.
//
// If a serveMux is not specified, opencensus metrics will automatically
// wrap the handler. It is recommended to configure opencensus yourself
// if specifying a serveMux.
func RegisterHTTPHandler(universe *gc.Universe, opts *HTTPOptions, serveMux *http.ServeMux) {
	basePath := defaultBasePath
	var recorder stats.Recorder
	if opts != nil {
		basePath = opts.BasePath
		recorder = opts.Recorder
	}
	newHTTPHandler := &HTTPHandler{
		basePath: basePath,
		universe: universe,
		recorder: recorder,
	}
	if serveMux == nil {
		http.Handle(basePath, &ochttp.Handler{
			Handler: ochttp.WithRouteTag(newHTTPHandler, basePath),
		})
	} else {
		serveMux.Handle(basePath, ochttp.WithRouteTag(newHTTPHandler, basePath))
	}
}

func (h *HTTPHandler) singleKeyServe(w http.ResponseWriter, r *http.Request, strippedPath string, needsUnescaping bool) {
	parts := strings.SplitN(strippedPath, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	galaxyName := parts[0]
	key := parts[1]

	if needsUnescaping {
		gn, gnUnescapeErr := url.PathUnescape(galaxyName)
		if gnUnescapeErr != nil {
			http.Error(w, fmt.Sprintf("failed to unescape galaxy name %q: %s", galaxyName, gnUnescapeErr), http.StatusBadRequest)
			return
		}
		k, keyUnescapeErr := url.PathUnescape(key)
		if keyUnescapeErr != nil {
			http.Error(w, fmt.Sprintf("failed to unescape key %q: %s", key, keyUnescapeErr), http.StatusBadRequest)
			return
		}
		galaxyName, key = gn, k
	}

	// Fetch the value for this galaxy/key.
	galaxy := h.universe.GetGalaxy(galaxyName)
	if galaxy == nil {
		http.Error(w, "no such galaxy: "+galaxyName, http.StatusNotFound)
		return
	}

	ctx := r.Context()

	// TODO: remove galaxy.Stats from here
	galaxy.Stats.ServerRequests.Add(1)
	if err := stats.RecordWithOptions(
		ctx,
		stats.WithMeasurements(gc.MServerRequests.M(1)),
		stats.WithRecorder(h.recorder),
	); err != nil {
		http.Error(w, "failed to record stats", http.StatusInternalServerError)
		return
	}

	var value gc.ByteCodec
	err := galaxy.Get(ctx, key, &value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	b, expTm, err := value.MarshalBinary()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(ttlHeader, fmt.Sprintf("%d", expTm.UnixMilli()))
	_, err = w.Write(b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *HTTPHandler) multipleKeyServe(w http.ResponseWriter, r *http.Request) {
	galaxyName := r.Header["Galaxy"][0]
	keys := r.Header["Key"]

	// Fetch the value for this galaxy/key.
	galaxy := h.universe.GetGalaxy(galaxyName)
	if galaxy == nil {
		http.Error(w, "no such galaxy: "+galaxyName, http.StatusNotFound)
		return
	}

	mw := multipart.NewWriter(w)
	w.Header().Add("Content-Type", mw.FormDataContentType())

	for _, key := range keys {
		ctx := r.Context()

		// TODO: remove galaxy.Stats from here
		galaxy.Stats.ServerRequests.Add(1)
		if err := stats.RecordWithOptions(
			ctx,
			stats.WithMeasurements(gc.MServerRequests.M(1)),
			stats.WithRecorder(h.recorder),
		); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var value gc.ByteCodec
		err := galaxy.Get(ctx, key, &value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		b, expTm, err := value.MarshalBinary()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		partw, err := mw.CreatePart(textproto.MIMEHeader{
			ttlHeader:        []string{fmt.Sprintf("%d", expTm.UnixMilli())},
			"Content-Length": []string{fmt.Sprintf("%d", len(b))},
		})
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(fmt.Sprintf("failed to create part: %s", err.Error())))
			return
		}
		_, err = partw.Write(b)
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(fmt.Sprintf("failed to write part: %s", err.Error())))
			return
		}
	}
	err := mw.Close()
	if err != nil {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(fmt.Sprintf("failed to close multi-part writer: %s", err.Error())))
		return
	}
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, h.basePath) {
		panic("HTTPHandler serving unexpected path: " + r.URL.Path)
	}
	strippedPath := r.URL.Path[len(h.basePath):]
	needsUnescaping := false
	if r.URL.RawPath != "" && r.URL.RawPath != r.URL.Path {
		strippedPath = r.URL.RawPath[len(h.basePath):]
		needsUnescaping = true
	}
	if strippedPath != "" {
		h.singleKeyServe(w, r, strippedPath, needsUnescaping)
		return
	}
	h.multipleKeyServe(w, r)

}

type httpFetcher struct {
	transport http.RoundTripper
	baseURL   string
}

// Fetch here implements the RemoteFetcher interface for sending a GET request over HTTP to a peer
func (h *httpFetcher) Fetch(ctx context.Context, galaxy string, keys []string) ([]galaxycache.ValueWithTTL, error) {
	req, err := http.NewRequest("GET", h.baseURL, http.NoBody)
	if err != nil {
		return []galaxycache.ValueWithTTL{}, err
	}
	req.Header["Key"] = keys
	req.Header["Galaxy"] = []string{galaxy}

	res, err := h.transport.RoundTrip(req.WithContext(ctx))
	if err != nil {
		return []galaxycache.ValueWithTTL{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return []galaxycache.ValueWithTTL{}, fmt.Errorf("server returned HTTP response status code: %v", res.Status)
	}

	_, params, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))

	mr := multipart.NewReader(res.Body, params["boundary"])
	// TODO(GiedriusS): convert this into a loop.
	part, err := mr.NextPart()
	if err != nil {
		return []galaxycache.ValueWithTTL{}, err
	}
	lengthHeader := part.Header["Content-Length"]

	var data []byte

	if len(lengthHeader) > 0 {
		lengthVal := lengthHeader[0]
		length, err := strconv.ParseInt(string(lengthVal), 10, 64)
		if err != nil {
			return []galaxycache.ValueWithTTL{}, fmt.Errorf("got non-integer Content-Length header (%v): %v", lengthHeader, err)
		}
		dataBuffer := make([]byte, length)

		_, err = part.Read(dataBuffer)
		if err != nil && err != io.EOF {
			return []galaxycache.ValueWithTTL{}, fmt.Errorf("reading part: %v", err)
		}

		data = dataBuffer
	} else {
		dataBuffer, err := ioutil.ReadAll(part)
		if err != nil {
			return []galaxycache.ValueWithTTL{}, fmt.Errorf("reading response body: %v", err)
		}

		data = dataBuffer
	}

	expireStr := part.Header.Get(ttlHeader)
	if len(expireStr) == 0 {
		return []galaxycache.ValueWithTTL{}, fmt.Errorf("failed reading TTL header %s", ttlHeader)
	}
	expire, err := strconv.ParseInt(string(expireStr), 10, 64)
	if err != nil {
		return []galaxycache.ValueWithTTL{}, fmt.Errorf("parsing TTL header %s: %w", ttlHeader, err)
	}
	return []galaxycache.ValueWithTTL{{Data: data, TTL: time.UnixMilli(expire)}}, nil
}

// Close here implements the RemoteFetcher interface for closing (does nothing for HTTP)
func (h *httpFetcher) Close() error {
	return nil
}

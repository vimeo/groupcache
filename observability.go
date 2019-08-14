/*
Copyright 2018 Google LLC.

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
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	unitDimensionless = "1"
	unitBytes         = "By"
	unitMillisecond   = "ms"
)

var (
	// Copied from https://github.com/census-instrumentation/opencensus-go/blob/ff7de98412e5c010eb978f11056f90c00561637f/plugin/ocgrpc/stats_common.go#L54
	defaultBytesDistribution = view.Distribution(0, 1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296)
	// Copied from https://github.com/census-instrumentation/opencensus-go/blob/ff7de98412e5c010eb978f11056f90c00561637f/plugin/ocgrpc/stats_common.go#L55
	defaultMillisecondsDistribution = view.Distribution(0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000)
)

// Opencensus stats
var (
	MGets            = stats.Int64("gets", "The number of Get requests", unitDimensionless)
	MCacheHits       = stats.Int64("cache_hits", "The number of times that the cache was good", unitDimensionless)
	MCacheMisses     = stats.Int64("cache_misses", "The number of times that either cache was not good", unitDimensionless)
	MPeerLoads       = stats.Int64("peer_loads", "The number of remote loads or remote cache hits", unitDimensionless)
	MPeerErrors      = stats.Int64("peer_errors", "The number of remote errors", unitDimensionless)
	MLoads           = stats.Int64("loads", "The number of gets/cacheHits", unitDimensionless)
	MLoadErrors      = stats.Int64("loads_errors", "The number of errors encountered during Get", unitDimensionless)
	MLoadsDeduped    = stats.Int64("loads_deduped", "The number of loads after singleflight", unitDimensionless)
	MLocalLoads      = stats.Int64("local_loads", "The number of good local loads", unitDimensionless)
	MLocalLoadErrors = stats.Int64("local_load_errors", "The number of bad local loads", unitDimensionless)
	MBackendLoads    = stats.Int64("backend_loads", "The number of good loads from the backend getter", unitDimensionless)

	MSFCacheHits    = stats.Int64("sf_cache_hits", "The number of times that the cache was good while in singleflight", unitDimensionless)
	MSFPeerLoads    = stats.Int64("sf_peer_loads", "The number of remote loads or remote cache hits in singleflight", unitDimensionless)
	MSFLocalLoads   = stats.Int64("sf_local_loads", "The number of good singleflighted local loads", unitDimensionless)
	MSFBackendLoads = stats.Int64("sf_backend_loads", "The number of good loads from the backend getter in singleflight", unitDimensionless)

	MServerRequests = stats.Int64("server_requests", "The number of Gets that came over the network from peers", unitDimensionless)
	MKeyLength      = stats.Int64("key_length", "The length of keys", unitBytes)
	MValueLength    = stats.Int64("value_length", "The length of values", unitBytes)

	MRoundtripLatencyMilliseconds = stats.Float64("roundtrip_latency", "Roundtrip latency in milliseconds", unitMillisecond)
)

// GalaxyKey tags the name of the galaxy
var GalaxyKey = tag.MustNewKey("galaxy")

// HitLevelKey tags the level at which data was found on Get
var CacheLevelKey = tag.MustNewKey("cache-hit-level")

// AllViews is a slice of default views for people to use
var AllViews = []*view.View{
	{Name: "galaxycache/gets", Description: "The number of Get requests", TagKeys: []tag.Key{GalaxyKey}, Measure: MGets, Aggregation: view.Count()},
	{Name: "galaxycache/cache_hits", Description: "The number of times that the cache was good", TagKeys: []tag.Key{GalaxyKey, CacheLevelKey}, Measure: MCacheHits, Aggregation: view.Count()},
	{Name: "galaxycache/cache_misses", Description: "The number of times that either cache was not good", TagKeys: []tag.Key{GalaxyKey}, Measure: MCacheMisses, Aggregation: view.Count()},
	{Name: "galaxycache/peer_loads", Description: "The number of remote loads or remote cache hits", TagKeys: []tag.Key{GalaxyKey}, Measure: MPeerLoads, Aggregation: view.Count()},
	{Name: "galaxycache/peer_errors", Description: "The number of remote errors", TagKeys: []tag.Key{GalaxyKey}, Measure: MPeerErrors, Aggregation: view.Count()},
	{Name: "galaxycache/loads", Description: "The number of loads after singleflight", TagKeys: []tag.Key{GalaxyKey}, Measure: MLoads, Aggregation: view.Count()},
	{Name: "galaxycache/loads_deduped", Description: "The number of loads after singleflight", TagKeys: []tag.Key{GalaxyKey}, Measure: MLoadsDeduped, Aggregation: view.Count()},
	{Name: "galaxycache/local_loads", Description: "The number of good local loads", TagKeys: []tag.Key{GalaxyKey}, Measure: MLocalLoads, Aggregation: view.Count()},
	{Name: "galaxycache/local_load_errors", Description: "The number of bad local loads", TagKeys: []tag.Key{GalaxyKey}, Measure: MLocalLoadErrors, Aggregation: view.Count()},
	{Name: "galaxycache/backend_loads", Description: "The number of good backend loads", TagKeys: []tag.Key{GalaxyKey}, Measure: MBackendLoads, Aggregation: view.Count()},

	{Name: "galaxycache/sf_cache_hits", Description: "The number of times that the cache was good while in singleflight", TagKeys: []tag.Key{GalaxyKey, CacheLevelKey}, Measure: MSFCacheHits, Aggregation: view.Count()},
	{Name: "galaxycache/sf_peer_loads", Description: "The number of remote loads or remote cache hits in singleflight", TagKeys: []tag.Key{GalaxyKey}, Measure: MSFPeerLoads, Aggregation: view.Count()},
	{Name: "galaxycache/sf_local_loads", Description: "The number of good singleflighted local loads", TagKeys: []tag.Key{GalaxyKey}, Measure: MSFLocalLoads, Aggregation: view.Count()},
	{Name: "galaxycache/sf_backend_loads", Description: "The number of good loads from the backend getter in singleflight", TagKeys: []tag.Key{GalaxyKey}, Measure: MSFBackendLoads, Aggregation: view.Count()},

	{Name: "galaxycache/server_requests", Description: "The number of Gets that came over the network from peers", TagKeys: []tag.Key{GalaxyKey}, Measure: MServerRequests, Aggregation: view.Count()},
	{Name: "galaxycache/key_length", Description: "The distribution of the key lengths", TagKeys: []tag.Key{GalaxyKey}, Measure: MKeyLength, Aggregation: defaultBytesDistribution},
	{Name: "galaxycache/value_length", Description: "The distribution of the value lengths", TagKeys: []tag.Key{GalaxyKey}, Measure: MValueLength, Aggregation: defaultBytesDistribution},
	{Name: "galaxycache/roundtrip_latency", Description: "The roundtrip latency", TagKeys: []tag.Key{GalaxyKey}, Measure: MRoundtripLatencyMilliseconds, Aggregation: defaultMillisecondsDistribution},
}

func sinceInMilliseconds(start time.Time) float64 {
	d := time.Since(start)
	return float64(d.Nanoseconds()) / 1e6
}

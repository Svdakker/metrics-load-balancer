package sharder

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/prometheus/prometheus/prompb"
)

type Sharder struct {
	mu       sync.RWMutex
	backends []string          // Keep a list of unique backends for map pre-allocation
	ringKeys []uint32          // Sorted array of hashes representing points on the ring
	ringMap  map[uint32]string // Maps the hash point to the actual backend URL
	vNodes   int               // Number of virtual nodes per backend for even distribution
}

func New(backends []string) *Sharder {
	s := &Sharder{
		backends: backends,
		ringMap:  make(map[uint32]string),
		vNodes:   1024,
	}

	for _, b := range backends {
		s.addBackend(b)
	}

	return s
}

func (s *Sharder) addBackend(backend string) {
	for i := 0; i < s.vNodes; i++ {
		// Create a unique key for the virtual node (e.g., "http://alloy:8080-0")
		vNodeKey := fmt.Sprintf("%s-%d", backend, i)
		hash := hashString(vNodeKey)

		s.ringKeys = append(s.ringKeys, hash)
		s.ringMap[hash] = backend
	}

	sort.Slice(s.ringKeys, func(i, j int) bool {
		return s.ringKeys[i] < s.ringKeys[j]
	})
}

func (s *Sharder) getBackend(hash uint32) string {
	if len(s.ringKeys) == 0 {
		return ""
	}

	idx := sort.Search(len(s.ringKeys), func(i int) bool {
		return s.ringKeys[i] >= hash
	})

	if idx == len(s.ringKeys) {
		idx = 0
	}

	return s.ringMap[s.ringKeys[idx]]
}

func (s *Sharder) Shard(req *prompb.WriteRequest) map[string]*prompb.WriteRequest {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sharded := make(map[string]*prompb.WriteRequest, len(s.backends))
	for _, b := range s.backends {
		sharded[b] = &prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{},
		}
	}

	if req == nil || len(s.backends) == 0 || len(req.Timeseries) == 0 {
		return sharded
	}

	for _, ts := range req.Timeseries {
		fp := fingerprint(ts.Labels)

		targetBackend := s.getBackend(fp)

		sharded[targetBackend].Timeseries = append(sharded[targetBackend].Timeseries, ts)
	}

	return sharded
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func fingerprint(labels []prompb.Label) uint32 {
	h := fnv.New32a()

	for _, l := range labels {
		h.Write([]byte(l.Name))
		h.Write([]byte{'\xff'})
		h.Write([]byte(l.Value))
		h.Write([]byte{'\xff'})
	}

	return h.Sum32()
}

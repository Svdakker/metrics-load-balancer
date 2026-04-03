package sharder

import (
	"hash/fnv"

	"github.com/prometheus/prometheus/prompb"
)

type Sharder struct {
	backends []string
}

func New(backends []string) *Sharder {
	return &Sharder{
		backends: backends,
	}
}

func (s *Sharder) Shard(req *prompb.WriteRequest) map[string]*prompb.WriteRequest {
	sharded := make(map[string]*prompb.WriteRequest, len(s.backends))
	for _, b := range s.backends {
		sharded[b] = &prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{},
		}
	}

	if req == nil || len(s.backends) == 0 {
		return sharded
	}

	if len(req.Timeseries) == 0 {
		return sharded
	}

	for _, ts := range req.Timeseries {
		fp := fingerprint(ts.Labels)

		idx := fp % uint32(len(s.backends))
		targetBackend := s.backends[idx]

		sharded[targetBackend].Timeseries = append(sharded[targetBackend].Timeseries, ts)
	}

	return sharded
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

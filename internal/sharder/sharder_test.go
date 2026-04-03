package sharder

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func TestFingerprint_Determinism(t *testing.T) {
	labelsA := []prompb.Label{{Name: "__name__", Value: "cpu"}, {Name: "pod", Value: "web"}}
	labelsB := []prompb.Label{{Name: "__name__", Value: "cpu"}, {Name: "pod", Value: "web"}}

	if fingerprint(labelsA) != fingerprint(labelsB) {
		t.Errorf("Identical labels must produce identical fingerprints")
	}
}

func TestFingerprint_Collisions(t *testing.T) {
	labels1 := []prompb.Label{{Name: "a", Value: "bc"}}
	labels2 := []prompb.Label{{Name: "ab", Value: "c"}}

	if fingerprint(labels1) == fingerprint(labels2) {
		t.Errorf("Fingerprint collision detected between {a=\"bc\"} and {ab=\"c\"}")
	}
}

func TestSharder_ZeroBackends(t *testing.T) {
	s := New([]string{})

	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{Labels: []prompb.Label{{Name: "test", Value: "1"}}},
		},
	}

	result := s.Shard(req)

	if len(result) != 0 {
		t.Errorf("Expected empty result map when zero backends are configured, got %d", len(result))
	}
}

func TestSharder_NilRequest(t *testing.T) {
	s := New([]string{"http://backend-1"})

	result := s.Shard(nil)

	if len(result) != 1 {
		t.Errorf("Expected result map to contain 1 backend, got %d", len(result))
	}
	if len(result["http://backend-1"].Timeseries) != 0 {
		t.Errorf("Expected 0 timeseries in backend, got %d", len(result["http://backend-1"].Timeseries))
	}
}

func TestSharder_EmptyTimeseries(t *testing.T) {
	s := New([]string{"http://backend-1", "http://backend-2"})

	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{},
	}

	result := s.Shard(req)

	if len(result) != 2 {
		t.Errorf("Expected map with 2 backends, got %d", len(result))
	}

	for backend, batch := range result {
		if len(batch.Timeseries) > 0 {
			t.Errorf("Expected backend %s to have 0 timeseries, got %d", backend, len(batch.Timeseries))
		}
	}
}

func TestSharder_Distribution(t *testing.T) {
	backends := []string{"A", "B", "C"}
	s := New(backends)

	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{Labels: []prompb.Label{{Name: "pod", Value: "1"}}},
			{Labels: []prompb.Label{{Name: "pod", Value: "2"}}},
			{Labels: []prompb.Label{{Name: "pod", Value: "3"}}},
			{Labels: []prompb.Label{{Name: "pod", Value: "4"}}},
			{Labels: []prompb.Label{{Name: "pod", Value: "5"}}},
		},
	}

	result := s.Shard(req)

	totalRouted := 0
	for _, batch := range result {
		totalRouted += len(batch.Timeseries)
	}

	if totalRouted != 5 {
		t.Errorf("Expected exactly 5 timeseries to be routed, got %d", totalRouted)
	}
}

func TestHashRing_Distribution(t *testing.T) {
	backends := []string{"http://node-1", "http://node-2", "http://node-3"}
	s := New(backends)

	distribution := make(map[string]int)

	req := &prompb.WriteRequest{}
	for i := 0; i < 10000; i++ {
		ts := prompb.TimeSeries{
			Labels: []prompb.Label{{Name: "test_idx", Value: fmt.Sprintf("%d", i)}},
		}
		req.Timeseries = append(req.Timeseries, ts)
	}

	result := s.Shard(req)

	for backend, batch := range result {
		distribution[backend] = len(batch.Timeseries)
	}

	expected := 10000 / len(backends)
	variance := float64(expected) * 0.15

	for backend, count := range distribution {
		if float64(count) < float64(expected)-variance || float64(count) > float64(expected)+variance {
			t.Errorf("Backend %s received %d metrics, expected ~%d. Variance too high.", backend, count, expected)
		}
	}
}

func TestHashRing_WrapAround(t *testing.T) {
	s := New([]string{"node-1"})

	s.ringKeys = []uint32{100, 200, 300}
	s.ringMap = map[uint32]string{
		100: "node-A",
		200: "node-B",
		300: "node-C",
	}

	if s.getBackend(200) != "node-B" {
		t.Errorf("Expected node-B")
	}

	if s.getBackend(250) != "node-C" {
		t.Errorf("Expected node-C")
	}

	if s.getBackend(350) != "node-A" {
		t.Errorf("Expected wrap-around to node-A, got %s", s.getBackend(350))
	}
}

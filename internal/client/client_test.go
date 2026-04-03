package client

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}

func TestClient_Decode(t *testing.T) {
	c := New()

	ts := prompb.TimeSeries{
		Labels: []prompb.Label{{Name: "test", Value: "decode"}},
	}
	writeReq := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{ts}}
	data, _ := writeReq.Marshal()
	compressed := snappy.Encode(nil, data)

	decoded, err := c.Decode(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded.Timeseries) != 1 || decoded.Timeseries[0].Labels[0].Value != "decode" {
		t.Errorf("Decoded data mismatch")
	}
}

func TestClient_Pack(t *testing.T) {
	c := New()
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{Labels: []prompb.Label{{Name: "a", Value: "b"}}}},
	}

	payload, err := c.Pack(req)
	if err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	decoded, err := c.Decode(bytes.NewReader(payload))
	if err != nil {
		t.Errorf("Pack produced un-decodable data: %v", err)
	}
	if len(decoded.Timeseries) != 1 {
		t.Errorf("Data corruption in Pack/Decode cycle")
	}
}

func TestClient_Push_Success(t *testing.T) {
	mockAlloy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") != "snappy" {
			t.Errorf("Missing Snappy header")
		}
		if r.Header.Get("X-Prometheus-Remote-Write-Version") != "0.1.0" {
			t.Errorf("Missing version header")
		}

		body, _ := io.ReadAll(r.Body)
		uncompressed, err := snappy.Decode(nil, body)
		if err != nil {
			t.Errorf("Server received invalid snappy: %v", err)
		}

		var req prompb.WriteRequest
		if err := req.Unmarshal(uncompressed); err != nil {
			t.Errorf("Server received invalid protobuf: %v", err)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer mockAlloy.Close()

	c := New()
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{Labels: []prompb.Label{{Name: "a", Value: "b"}}}},
	}

	payload, err := c.Pack(req)
	if err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	err = c.Push(context.Background(), mockAlloy.URL, payload)
	if err != nil {
		t.Errorf("Push failed: %v", err)
	}
}

func TestClient_Push_ErrorHandling(t *testing.T) {
	mockErrorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal database error", http.StatusInternalServerError)
	}))
	defer mockErrorServer.Close()

	c := New()
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{Labels: []prompb.Label{{Name: "a", Value: "b"}}}},
	}

	payload, err := c.Pack(req)
	if err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	err = c.Push(context.Background(), mockErrorServer.URL, payload)
	if err == nil {
		t.Fatal("Expected error from 500 status, got nil")
	}

	expectedSnippet := "remote write returned 500"
	if !contains(err.Error(), expectedSnippet) {
		t.Errorf("Error string mismatch. Got: %v, Want snippet: %v", err.Error(), expectedSnippet)
	}
}

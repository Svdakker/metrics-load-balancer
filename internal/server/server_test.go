package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/prometheus/prompb"

	"github.com/Svdakker/metrics-load-balancer/internal/client"
	"github.com/Svdakker/metrics-load-balancer/internal/sharder"
)

func TestHealthCheck(t *testing.T) {

	srv := New("8080", nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	srv.healthCheck(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("health returned %v", status)
	}
}

func TestHandleRequest_InvalidMethod(t *testing.T) {
	s := sharder.New([]string{"http://localhost:9090"})
	c := client.New()
	srv := New("8080", s, c)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/write", nil)
	rr := httptest.NewRecorder()

	srv.handleRequest(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("handler returned wrong status code: got %v expected %v",
			status, http.StatusMethodNotAllowed)
	}
}

func TestHandleRequest_ValidPayload(t *testing.T) {
	mockBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockBackend.Close()

	s := sharder.New([]string{mockBackend.URL})
	c := client.New()
	srv := New("8080", s, c)

	testReq := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
				Samples: []prompb.Sample{{Value: 1.0, Timestamp: 1670000000000}},
			},
		},
	}
	payload, _ := c.Pack(testReq)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/metrics/write", bytes.NewReader(payload))
	rr := httptest.NewRecorder()

	srv.handleRequest(rr, req)

	if status := rr.Code; status != http.StatusAccepted {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusAccepted)
	}
}

func TestHandleRequest_InvalidSnappy(t *testing.T) {
	s := sharder.New([]string{"http://localhost:9090"})
	c := client.New()
	srv := New("8080", s, c)

	garbageData := []byte("this is not compressed data")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/metrics/write", bytes.NewReader(garbageData))
	rr := httptest.NewRecorder()

	srv.handleRequest(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code for invalid snappy: got %v expected %v", status, http.StatusBadRequest)
	}
}

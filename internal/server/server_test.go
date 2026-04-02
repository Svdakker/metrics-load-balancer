package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func TestHealthCheck(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	rr := httptest.NewRecorder()

	healthCheck(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	expected := "OK"
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestHandleRequest_InvalidMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/write", nil)
	rr := httptest.NewRecorder()

	handleRequest(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("handler returned wrong status code for GET: got %v want %v",
			status, http.StatusMethodNotAllowed)
	}
}

func TestHandleRequest_ValidPayload(t *testing.T) {
	reqProto := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "test_metric"},
					{Name: "account_id", Value: "12345"},
				},
				Samples: []prompb.Sample{
					{Value: 1.0, Timestamp: 1670000000000},
				},
			},
		},
	}

	data, err := reqProto.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal proto: %v", err)
	}

	compressed := snappy.Encode(nil, data)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/metrics/write", bytes.NewReader(compressed))
	rr := httptest.NewRecorder()

	handleRequest(rr, req)

	if status := rr.Code; status != http.StatusAccepted {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusAccepted)
	}
}

func TestHandleRequest_InvalidSnappy(t *testing.T) {
	garbageData := []byte("this is not compressed data")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/metrics/write", bytes.NewReader(garbageData))
	rr := httptest.NewRecorder()

	handleRequest(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code for invalid snappy: got %v want %v", status, http.StatusBadRequest)
	}
}

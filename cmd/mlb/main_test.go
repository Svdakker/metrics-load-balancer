package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
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

func TestHandleRequest_ValidMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/metrics/write", nil)
	rr := httptest.NewRecorder()

	handleRequest(rr, req)

	if status := rr.Code; status != http.StatusAccepted {
		t.Errorf("handler returned wrong status code for POST: got %v want %v",
			status, http.StatusAccepted)
	}
}

func TestHandlePush_InvalidMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/write", nil)
	rr := httptest.NewRecorder()

	handleRequest(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("handler returned wrong status code for GET: got %v want %v",
			status, http.StatusMethodNotAllowed)
	}
}

package dispatcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/prometheus/prompb"

	"github.com/Svdakker/metrics-load-balancer/internal/client"
)

func TestDispatcher_WorkerPoolSuccess(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	c := client.New()

	d := New(c, 1, 5)
	d.Start()

	resultChan := make(chan error, 1)
	job := Job{
		Ctx:    context.Background(),
		Target: mockServer.URL,
		Payload: &prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{{Labels: []prompb.Label{{Name: "test", Value: "1"}}}},
		},
		Result: resultChan,
	}

	d.Submit(job)

	err := <-resultChan

	if err != nil {
		t.Fatalf("Expected successful push, got error: %v", err)
	}
}

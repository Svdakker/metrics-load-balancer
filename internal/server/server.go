package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Svdakker/metrics-load-balancer/internal/client"
	"github.com/Svdakker/metrics-load-balancer/internal/dispatcher"
	"github.com/Svdakker/metrics-load-balancer/internal/sharder"
)

type HttpReceiver struct {
	httpServer *http.Server
	sharder    *sharder.Sharder
	client     *client.Client
	dispatcher *dispatcher.Dispatcher
}

func New(port string, s *sharder.Sharder, c *client.Client, d *dispatcher.Dispatcher) *HttpReceiver {
	receiver := &HttpReceiver{
		sharder:    s,
		client:     c,
		dispatcher: d,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", receiver.healthCheck)
	mux.HandleFunc("/api/v1/metrics/write", receiver.handleRequest)

	receiver.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return receiver
}

func (s *HttpReceiver) Start() {
	go func() {
		log.Printf("Starting metrics-load-balancer on port %s...", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s: %v\n", s.httpServer.Addr, err)
		}
	}()

	s.waitForShutdown()
}

func (s *HttpReceiver) waitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutdown signal received. Shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited cleanly.")
}

func (s *HttpReceiver) healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *HttpReceiver) handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	req, err := s.client.Decode(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("INCOMING: Received %d timeseries from %s", len(req.Timeseries), r.RemoteAddr)

	shardedBatches := s.sharder.Shard(req)
	errChan := make(chan error, len(shardedBatches))
	jobsSubmitted := 0

	for url, batch := range shardedBatches {
		if len(batch.Timeseries) == 0 {
			continue
		}

		log.Printf("FORWARDING: Queueing %d timeseries for %s", len(batch.Timeseries), url)

		jobsSubmitted++
		s.dispatcher.Submit(dispatcher.Job{
			Ctx:     r.Context(),
			Target:  url,
			Payload: batch,
			Result:  errChan,
		})
	}

	var finalErr error
	for i := 0; i < jobsSubmitted; i++ {
		if err := <-errChan; err != nil {
			finalErr = err
		}
	}

	if finalErr != nil {
		log.Printf("Dispatch failure: %v", finalErr)
		http.Error(w, "Error forwarding metrics", http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

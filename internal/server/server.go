package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type HttpReceiver struct {
	httpServer *http.Server
}

func New(port string) *HttpReceiver {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthCheck)
	mux.HandleFunc("/api/v1/metrics/write", handleRequest)

	return &HttpReceiver{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%s", port),
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
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

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed, only POST method is supported", http.StatusMethodNotAllowed)
		return
	}

	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, "Failed to decompress Snappy payload", http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := req.Unmarshal(uncompressed); err != nil {
		http.Error(w, "Failed to unmarshal Protobuf", http.StatusBadRequest)
		return
	}

	log.Printf("Successfully unpacked %d timeseries", len(req.Timeseries))

	w.WriteHeader(http.StatusAccepted)
}

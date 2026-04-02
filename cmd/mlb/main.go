package main

import (
	"os"

	"github.com/Svdakker/metrics-load-balancer/internal/server"
)

func main() {
	port := os.Getenv("MLB_LISTEN_PORT")
	if port == "" {
		port = "8080"
	}

	receiver := server.New(port)
	receiver.Start()
}

package main

import (
	"os"

	"github.com/Svdakker/metrics-load-balancer/internal/client"
	"github.com/Svdakker/metrics-load-balancer/internal/server"
	"github.com/Svdakker/metrics-load-balancer/internal/sharder"
)

func main() {
	port := os.Getenv("MLB_LISTEN_PORT")
	if port == "" {
		port = "8081"
	}

	backends := []string{
		"http://localhost:8080/api/v1/metrics/write",
	}

	routerSharder := sharder.New(backends)
	routerClient := client.New()

	receiver := server.New(port, routerSharder, routerClient)
	receiver.Start()
}

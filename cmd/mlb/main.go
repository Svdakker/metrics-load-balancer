package main

import (
	"os"

	"github.com/Svdakker/metrics-load-balancer/internal/client"
	"github.com/Svdakker/metrics-load-balancer/internal/dispatcher"
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
		"http://localhost:8082/api/v1/metrics/write",
	}

	routerSharder := sharder.New(backends)
	routerClient := client.New()

	routerDispatcher := dispatcher.New(routerClient, 50, 1000)
	routerDispatcher.Start()

	receiver := server.New(port, routerSharder, routerClient, routerDispatcher)
	receiver.Start()
}

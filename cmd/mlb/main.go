package main

import (
	"log/slog"

	"github.com/Svdakker/metrics-load-balancer/internal/client"
	"github.com/Svdakker/metrics-load-balancer/internal/config"
	"github.com/Svdakker/metrics-load-balancer/internal/dispatcher"
	"github.com/Svdakker/metrics-load-balancer/internal/logger"
	"github.com/Svdakker/metrics-load-balancer/internal/server"
	"github.com/Svdakker/metrics-load-balancer/internal/sharder"
)

func main() {
	cfg := config.Load()

	logger.Init(cfg.LogLevel)
	slog.Info("Starting metrics router",
		"port", cfg.Port,
		"backends", len(cfg.Backends),
		"workers", cfg.WorkerCount,
	)

	routerSharder := sharder.New(cfg.Backends)
	routerClient := client.New()

	routerDispatcher := dispatcher.New(routerClient, cfg.WorkerCount, cfg.BufferSize)
	routerDispatcher.Start()

	receiver := server.New(cfg.Port, routerSharder, routerClient, routerDispatcher)
	receiver.Start()
}

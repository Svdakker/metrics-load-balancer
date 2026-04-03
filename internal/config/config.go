package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port        string
	Backends    []string
	LogLevel    string
	WorkerCount int
	BufferSize  int
}

func Load() *Config {
	return &Config{
		Port:        getEnv("MLB_LISTEN_PORT", "8080"),
		Backends:    getEnvAsSlice("MLB_TARGET_BACKENDS", []string{"http://localhost:8080/api/v1/push"}),
		LogLevel:    getEnv("MLB_LOG_LEVEL", "info"),
		WorkerCount: getEnvAsInt("MLB_WORKER_COUNT", 50),
		BufferSize:  getEnvAsInt("MLB_BUFFER_SIZE", 1000),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvAsSlice(key string, fallback []string) []string {
	if value, exists := os.LookupEnv(key); exists {
		return strings.Split(value, ",")
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return fallback
}

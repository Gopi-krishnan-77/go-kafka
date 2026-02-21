// Package config loads and validates application configuration from
// environment variables with sensible defaults for local development.
package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Config holds all application-level configuration.
type Config struct {
	// HTTP
	Port string

	// Broker
	BrokerPartitions int

	// Pipeline
	PipelineTimeout time.Duration

	// Rate limiting
	RateLimitRPS   float64
	RateLimitBurst int

	// Storage
	TTL time.Duration

	// Worker
	WorkerCount int

	// Feature flags
	MetricsEnabled bool
}

// Load reads configuration from environment variables with defaults.
func Load() Config {
	cfg := Config{
		Port:             envOrDefault("PORT", "8080"),
		BrokerPartitions: envOrDefaultInt("BROKER_PARTITIONS", 4),
		PipelineTimeout:  envOrDefaultDuration("PIPELINE_TIMEOUT", 5*time.Second),
		RateLimitRPS:     envOrDefaultFloat("RATE_LIMIT_RPS", 50),
		RateLimitBurst:   envOrDefaultInt("RATE_LIMIT_BURST", 100),
		TTL:              envOrDefaultDuration("EVENT_TTL", 24*time.Hour),
		WorkerCount:      envOrDefaultInt("WORKER_COUNT", 4),
		MetricsEnabled:   envOrDefaultBool("METRICS_ENABLED", true),
	}
	cfg.validate()
	return cfg
}

func (c Config) validate() {
	if c.BrokerPartitions < 1 {
		log.Fatal("BROKER_PARTITIONS must be >= 1")
	}
	if c.RateLimitRPS <= 0 {
		log.Fatal("RATE_LIMIT_RPS must be > 0")
	}
	if c.WorkerCount < 1 {
		log.Fatal("WORKER_COUNT must be >= 1")
	}
	p, err := strconv.Atoi(c.Port)
	if err != nil || p < 1 || p > 65535 {
		log.Fatalf("PORT must be a valid port number, got %q", c.Port)
	}
}

// Addr returns the HTTP listen address.
func (c Config) Addr() string {
	return fmt.Sprintf(":%s", c.Port)
}

// --- helpers ---

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("config: invalid int for %s=%q, using default %d", key, v, fallback)
		return fallback
	}
	return n
}

func envOrDefaultFloat(key string, fallback float64) float64 {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Printf("config: invalid float for %s=%q, using default %f", key, v, fallback)
		return fallback
	}
	return f
}

func envOrDefaultBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Printf("config: invalid bool for %s=%q, using default %v", key, v, fallback)
		return fallback
	}
	return b
}

func envOrDefaultDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("config: invalid duration for %s=%q, using default %s", key, v, fallback)
		return fallback
	}
	return d
}

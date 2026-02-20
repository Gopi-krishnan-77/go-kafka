package config

import (
	"fmt"
	"os"
)

// Config holds application-level configuration loaded from environment variables.
type Config struct {
	Broker string
	Topic  string
	Port   string
	Group  string
}

// Load returns a Config with sane defaults for local development.
func Load() Config {
	return Config{
		Broker: envOrDefault("KAFKA_BROKER", "localhost:9092"),
		Topic:  envOrDefault("KAFKA_TOPIC", "weekend-events"),
		Port:   envOrDefault("PORT", "8080"),
		Group:  envOrDefault("KAFKA_GROUP", "weekend-workers"),
	}
}

func envOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// Addr returns the HTTP address for the API server.
func (c Config) Addr() string {
	return fmt.Sprintf(":%s", c.Port)
}

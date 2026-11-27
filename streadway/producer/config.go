package main

import (
	"log"
	"os"
	"strconv"
)

// Routing key tidak ada di sini: dibaca dari flag -key (default env
// RABBITMQ_ROUTING_KEY) di main.go.
type config struct {
	Host         string
	Exchange     string
	ExchangeType string
	MaxRetries   int
}

func loadConfig() config {
	return config{
		Host:         mustEnv("RABBITMQ_HOST"),
		Exchange:     mustEnv("RABBITMQ_EXCHANGE"),
		ExchangeType: mustEnv("RABBITMQ_EXCHANGE_TYPE"),
		MaxRetries:   envInt("RABBITMQ_MAX_RETRIES", 5),
	}
}

// envInt membaca env integer opsional; pakai def jika kosong.
func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		log.Fatalf("env %s harus bilangan bulat >= 0, dapat %q", key, v)
	}
	return n
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s belum di-set (jalankan: source .env)", key)
	}
	return v
}

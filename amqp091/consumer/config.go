package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

type config struct {
	Host         string
	Exchange     string
	ExchangeType string
	Queue        string
	BindingKey   string
	MaxRetries   int

	// Dead letter, diturunkan dari nama exchange/queue (tidak perlu env baru)
	DLX string // <exchange>.dlx
	DLQ string // <queue>.dlq

	// Retry queue ber-TTL untuk error sementara
	RetryQueue string        // <queue>.retry
	RetryMax   int           // maksimal retry sebelum ke DLQ
	RetryDelay time.Duration // TTL retry queue = jeda antar percobaan
}

func loadConfig() config {
	cfg := config{
		Host:         mustEnv("RABBITMQ_HOST"),
		Exchange:     mustEnv("RABBITMQ_EXCHANGE"),
		ExchangeType: mustEnv("RABBITMQ_EXCHANGE_TYPE"),
		Queue:        mustEnv("RABBITMQ_QUEUE"),
		BindingKey:   mustEnv("RABBITMQ_BINDING_KEY"),
		MaxRetries:   envInt("RABBITMQ_MAX_RETRIES", 5),
		RetryMax:     envInt("RABBITMQ_RETRY_MAX", 3),
		RetryDelay:   envDuration("RABBITMQ_RETRY_DELAY", 10*time.Second),
	}
	cfg.DLX = cfg.Exchange + ".dlx"
	cfg.DLQ = cfg.Queue + ".dlq"
	cfg.RetryQueue = cfg.Queue + ".retry"
	return cfg
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

// envDuration membaca env durasi opsional (format Go, mis. "10s", "1m"); pakai def jika kosong.
func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Fatalf("env %s harus durasi > 0 (mis. 10s), dapat %q", key, v)
	}
	return d
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s belum di-set (jalankan: source .env)", key)
	}
	return v
}

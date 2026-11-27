package main

import (
	"log"
	"os"
	"strconv"
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
}

func loadConfig() config {
	cfg := config{
		Host:         mustEnv("RABBITMQ_HOST"),
		Exchange:     mustEnv("RABBITMQ_EXCHANGE"),
		ExchangeType: mustEnv("RABBITMQ_EXCHANGE_TYPE"),
		Queue:        mustEnv("RABBITMQ_QUEUE"),
		BindingKey:   mustEnv("RABBITMQ_BINDING_KEY"),
		MaxRetries:   envInt("RABBITMQ_MAX_RETRIES", 5),
	}
	cfg.DLX = cfg.Exchange + ".dlx"
	cfg.DLQ = cfg.Queue + ".dlq"
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

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s belum di-set (jalankan: source .env)", key)
	}
	return v
}

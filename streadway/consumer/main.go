package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	minBackoff = 1 * time.Second
	maxBackoff = 30 * time.Second
)

func main() {
	cfg := loadConfig()

	// ctx ini milik aplikasi kita (streadway sendiri tidak mengenal context);
	// cancel saat Ctrl+C / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Loop reconnect: streadway TIDAK punya auto-reconnect. Setiap kali koneksi
	// atau channel putus, buat ulang semuanya dari nol (dial -> channel ->
	// declare -> bind -> qos -> consume) dengan exponential backoff,
	// maksimal cfg.MaxRetries percobaan berturut-turut yang gagal.
	// Dibuat sekali di luar loop supaya ingatan dedup bertahan lintas reconnect
	// (duplikat justru sering muncul tepat setelah reconnect).
	seen := newDedup()

	backoff := minBackoff
	retries := 0
	for {
		consumed, err := run(ctx, cfg, seen)
		if ctx.Err() != nil {
			log.Println("shutdown selesai")
			return
		}

		// Sempat consume = koneksi sebelumnya sehat -> reset backoff & hitungan retry
		if consumed {
			backoff = minBackoff
			retries = 0
		}

		if retries >= cfg.MaxRetries {
			// Exit non-zero supaya supervisor (systemd/Docker/k8s) yang me-restart
			log.Printf("terputus: %v; menyerah setelah %d percobaan reconnect", err, retries)
			os.Exit(1)
		}
		retries++
		log.Printf("terputus: %v; reconnect %d/%d dalam %s", err, retries, cfg.MaxRetries, backoff)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			log.Println("shutdown selesai")
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

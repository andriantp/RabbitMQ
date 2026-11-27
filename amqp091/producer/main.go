package main

import (
	"context"
	"flag"
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

	// Contoh:
	//   go run ./producer                                     # kirim tiap 2s
	//   go run ./producer -interval 500ms -key app.v2.order.created
	//   go run ./producer -body '{"id":99}'                   # body tetap
	key := flag.String("key", cfg.RoutingKey, "routing key (default: RABBITMQ_ROUTING_KEY)")
	body := flag.String("body", "", "isi pesan tetap (default: JSON contoh dengan id berurutan)")
	interval := flag.Duration("interval", 2*time.Second, "jeda antar publish")
	flag.Parse()

	// Cancel saat Ctrl+C / SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := &state{key: *key, body: *body, interval: *interval}

	// Loop reconnect, sama seperti consumer: exponential backoff, maksimal
	// cfg.MaxRetries percobaan gagal berturut-turut, lalu exit(1).
	backoff := minBackoff
	retries := 0
	for {
		connected, err := run(ctx, cfg, st)
		if ctx.Err() != nil {
			log.Printf("shutdown selesai (total terkirim: %d)", st.sent)
			return
		}

		// Sempat terhubung = koneksi sebelumnya sehat -> reset backoff & hitungan retry
		if connected {
			backoff = minBackoff
			retries = 0
		}

		if retries >= cfg.MaxRetries {
			log.Printf("terputus: %v; menyerah setelah %d percobaan reconnect", err, retries)
			os.Exit(1)
		}
		retries++
		log.Printf("terputus: %v; reconnect %d/%d dalam %s", err, retries, cfg.MaxRetries, backoff)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			log.Printf("shutdown selesai (total terkirim: %d)", st.sent)
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

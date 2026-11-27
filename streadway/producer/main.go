package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/rabbitmq/reconnect"
)

func main() {
	// Contoh:
	//   go run ./producer                                     # kirim tiap 2s
	//   go run ./producer -interval 500ms -key app.v2.order.created
	//   go run ./producer -body '{"id":99}'                   # body tetap
	//
	// Flag diparse SEBELUM loadConfig supaya -h jalan tanpa env, dan -key bisa
	// menggantikan RABBITMQ_ROUTING_KEY (env hanya jadi nilai default).
	key := flag.String("key", os.Getenv("RABBITMQ_ROUTING_KEY"), "routing key (default: $RABBITMQ_ROUTING_KEY)")
	body := flag.String("body", "", "isi pesan tetap (default: JSON contoh dengan id berurutan)")
	interval := flag.Duration("interval", 2*time.Second, "jeda antar publish")
	flag.Parse()
	if *key == "" {
		log.Fatal("routing key kosong: set RABBITMQ_ROUTING_KEY atau pakai -key")
	}
	if *interval <= 0 {
		log.Fatalf("-interval harus > 0, dapat %s", *interval)
	}

	cfg := loadConfig()

	// Cancel saat Ctrl+C / SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// state bertahan lintas reconnect (seq, pending, total terkirim)
	st := &state{key: *key, body: *body, interval: *interval}

	// Loop reconnect, sama seperti consumer. Sempat terhubung = sesi sehat.
	err := reconnect.Run(ctx, cfg.MaxRetries, func(ctx context.Context) (bool, error) {
		return run(ctx, cfg, st)
	})
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
	log.Printf("shutdown selesai (total terkirim: %d)", st.sent)
}

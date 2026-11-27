package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"example.com/rabbitmq/reconnect"
)

func main() {
	cfg := loadConfig()

	// ctx dipakai untuk loop reconnect kita dan diteruskan ke ConsumeWithContext;
	// cancel saat Ctrl+C / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Dibuat sekali di luar loop supaya ingatan dedup bertahan lintas reconnect
	// (duplikat justru sering muncul tepat setelah reconnect).
	seen := newDedup()

	// Loop reconnect: amqp091 TIDAK punya auto-reconnect. Setiap kali koneksi
	// atau channel putus, run() membuat ulang semuanya dari nol (dial -> channel ->
	// declare -> bind -> qos -> consume). Sempat consume = sesi sehat.
	err := reconnect.Run(ctx, cfg.MaxRetries, func(ctx context.Context) (bool, error) {
		return run(ctx, cfg, seen)
	})
	if err != nil {
		// Exit non-zero supaya supervisor (systemd/Docker/k8s) yang me-restart
		log.Print(err)
		os.Exit(1)
	}
	log.Println("shutdown selesai")
}

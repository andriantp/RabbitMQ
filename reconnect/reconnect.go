// Package reconnect berisi loop reconnect yang dipakai bersama oleh consumer
// dan producer (streadway maupun amqp091). Kedua library tidak punya
// auto-reconnect, jadi loop ini yang membuat ulang sesi setiap kali putus.
//
// Package ini sengaja tidak meng-import library AMQP mana pun: sesi
// (dial, channel, declare, consume/publish) disediakan pemanggil lewat Session.
package reconnect

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	MinBackoff = 1 * time.Second
	MaxBackoff = 30 * time.Second
)

// Session menjalankan satu sesi koneksi sampai putus (return error) atau ctx
// cancel. healthy=true jika sesi sempat sehat (mis. sudah consume / terhubung),
// yang me-reset backoff dan hitungan retry.
type Session func(ctx context.Context) (healthy bool, err error)

// Run memanggil session berulang kali dengan exponential backoff
// (MinBackoff -> x2 -> ... -> MaxBackoff), maksimal maxRetries percobaan gagal
// berturut-turut.
//
// Return nil saat ctx cancel (shutdown normal), atau error saat menyerah;
// pemanggil sebaiknya exit non-zero supaya supervisor (systemd/Docker/k8s)
// yang me-restart.
func Run(ctx context.Context, maxRetries int, session Session) error {
	return run(ctx, maxRetries, MinBackoff, MaxBackoff, session)
}

func run(ctx context.Context, maxRetries int, minBackoff, maxBackoff time.Duration, session Session) error {
	backoff := minBackoff
	retries := 0
	for {
		healthy, err := session(ctx)
		if ctx.Err() != nil {
			return nil
		}

		// Sesi sempat sehat -> reset backoff & hitungan retry
		if healthy {
			backoff = minBackoff
			retries = 0
		}

		if retries >= maxRetries {
			return fmt.Errorf("terputus: %v; menyerah setelah %d percobaan reconnect", err, retries)
		}
		retries++
		log.Printf("terputus: %v; reconnect %d/%d dalam %s", err, retries, maxRetries, backoff)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

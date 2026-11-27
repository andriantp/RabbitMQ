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

	// StableAfter: sesi sehat baru dianggap "pulih" (backoff & hitungan retry
	// di-reset) jika bertahan minimal selama ini. Tanpa batas ini, sesi yang
	// sempat sehat lalu langsung putus lagi (mis. channel ditutup broker karena
	// error yang selalu terulang) akan reconnect selamanya dan tidak pernah exit.
	StableAfter = 10 * time.Second
)

// Session menjalankan satu sesi koneksi sampai putus (return error) atau ctx
// cancel. healthy=true jika sesi sempat sehat (mis. sudah consume / terhubung).
type Session func(ctx context.Context) (healthy bool, err error)

type policy struct {
	minBackoff, maxBackoff, stableAfter time.Duration
}

// Run memanggil session berulang kali dengan exponential backoff
// (MinBackoff -> x2 -> ... -> MaxBackoff), maksimal maxRetries percobaan gagal
// berturut-turut. Sesi dihitung gagal kecuali sehat DAN bertahan >= StableAfter.
//
// Return nil saat ctx cancel (shutdown normal), atau error saat menyerah;
// pemanggil sebaiknya exit non-zero supaya supervisor (systemd/Docker/k8s)
// yang me-restart.
func Run(ctx context.Context, maxRetries int, session Session) error {
	return run(ctx, maxRetries, policy{MinBackoff, MaxBackoff, StableAfter}, session)
}

func run(ctx context.Context, maxRetries int, p policy, session Session) error {
	backoff := p.minBackoff
	retries := 0
	for {
		start := time.Now()
		healthy, err := session(ctx)
		if ctx.Err() != nil {
			return nil
		}

		// Sesi sehat dan cukup lama -> koneksi dianggap pulih: reset backoff & retry
		if lasted := time.Since(start); healthy {
			if lasted >= p.stableAfter {
				backoff = p.minBackoff
				retries = 0
			} else {
				log.Printf("sesi hanya bertahan %s (< %s), tetap dihitung sebagai percobaan gagal",
					lasted.Round(time.Millisecond), p.stableAfter)
			}
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
		backoff = min(backoff*2, p.maxBackoff)
	}
}

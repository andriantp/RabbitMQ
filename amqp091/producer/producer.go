package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"
)

// state bertahan lintas reconnect.
type state struct {
	key      string
	body     string // kosong = generate JSON contoh
	interval time.Duration

	seq     int      // id pesan terakhir yang dibuat
	pending *message // pesan yang gagal terkirim, dikirim ulang setelah reconnect
	sent    int
}

// message: MessageId dibuat SEKALI saat pesan dibuat, sehingga pengiriman ulang
// (pending) membawa id yang sama dan bisa dikenali consumer sebagai duplikat.
type message struct {
	id   string
	body []byte
}

// next mengembalikan pesan berikutnya: pending (jika ada) atau pesan baru.
func (s *state) next() message {
	if s.pending != nil {
		return *s.pending
	}
	s.seq++
	body := []byte(s.body)
	if s.body == "" {
		body = fmt.Appendf(nil, `{"id":%d,"created_at":%q}`, s.seq, time.Now().Format(time.RFC3339))
	}
	return message{id: newMessageID(), body: body}
}

// newMessageID menghasilkan id acak 128-bit (hex), unik antar producer & restart.
func newMessageID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand praktis tidak pernah gagal
	}
	return hex.EncodeToString(b)
}

// run menjalankan satu sesi: connect lalu publish periodik sampai koneksi
// putus (return error) atau ctx cancel (return nil).
// connected=true jika sesi ini sempat terhubung.
func run(ctx context.Context, cfg config, st *state) (connected bool, err error) {
	p, err := newPublisher(cfg)
	if err != nil {
		return false, err
	}
	defer p.Close()

	log.Printf("terhubung: exchange=%q routingKey=%q interval=%s", cfg.Exchange, st.key, st.interval)

	ticker := time.NewTicker(st.interval)
	defer ticker.Stop()

	// Setelah reconnect, langsung kirim ulang pesan pending tanpa menunggu tick
	if st.pending != nil {
		if err := publishOne(ctx, p, st); err != nil {
			return true, err
		}
	}

	for {
		select {
		case <-ticker.C:
			if err := publishOne(ctx, p, st); err != nil {
				return true, err
			}

		// Deteksi putus walau sedang idle di antara tick
		case e := <-p.connClosed:
			return true, fmt.Errorf("koneksi tertutup: %v", e)

		case e := <-p.chClosed:
			return true, fmt.Errorf("channel tertutup: %v", e)

		case <-ctx.Done():
			log.Println("shutdown...")
			return true, nil
		}
	}
}

// publishOne mengirim satu pesan. Error koneksi dikembalikan (memicu reconnect)
// dan pesan disimpan sebagai pending; unroutable/NACK hanya di-log.
func publishOne(ctx context.Context, p *publisher, st *state) error {
	msg := st.next()

	err := p.Publish(ctx, st.key, msg)
	switch {
	case err == nil:
		st.pending = nil
		st.sent++
		log.Printf("terkirim #%d: routingKey=%q messageId=%s body=%s", st.sent, st.key, msg.id, msg.body)
		return nil

	case errors.Is(err, errUnroutable), errors.Is(err, errNack):
		// Broker sehat tapi pesan tidak diterima: bukan alasan untuk reconnect.
		// Pesan dibuang (tidak di-retry) supaya tidak berulang terus.
		st.pending = nil
		log.Printf("gagal (dibuang): %v messageId=%s body=%s", err, msg.id, msg.body)
		return nil

	default:
		// Koneksi/channel bermasalah: simpan untuk dikirim ulang setelah reconnect.
		// Catatan: jika pesan sebenarnya sudah sampai tapi confirm-nya hilang,
		// pengiriman ulang menghasilkan duplikat (at-least-once).
		st.pending = &msg
		return err
	}
}

package main

import (
	"sync"
	"time"
)

// dedupTTL: berapa lama MessageId diingat. Harus lebih lama dari jeda terburuk
// antara pesan asli dan duplikatnya (reconnect producer + backoff).
const dedupTTL = 10 * time.Minute

// dedup mencatat MessageId yang sudah berhasil diproses (in-memory).
//
// Batasan: hilang saat consumer restart, dan tidak dibagi antar instance
// consumer. Untuk produksi, pakai penyimpanan bersama, mis. Redis
// SET key NX EX, atau unique constraint di database.
type dedup struct {
	mu        sync.Mutex
	seen      map[string]time.Time // MessageId -> waktu diproses
	lastPurge time.Time
}

func newDedup() *dedup {
	return &dedup{seen: make(map[string]time.Time), lastPurge: time.Now()}
}

// Seen: true jika id sudah pernah diproses dan belum kedaluwarsa.
func (d *dedup) Seen(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.seen[id]
	return ok && time.Since(t) < dedupTTL
}

// Mark mencatat id sebagai sudah diproses. Dipanggil setelah handler sukses.
func (d *dedup) Mark(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	d.seen[id] = now

	// Bersihkan entri kedaluwarsa sesekali supaya map tidak tumbuh terus
	if now.Sub(d.lastPurge) > dedupTTL {
		for k, t := range d.seen {
			if now.Sub(t) >= dedupTTL {
				delete(d.seen, k)
			}
		}
		d.lastPurge = now
	}
}

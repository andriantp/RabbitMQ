package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// errTransient menandai error sementara (DB down, timeout, ...): pesan dicoba
// lagi lewat retry queue. Error lain dianggap permanen dan langsung ke DLQ.
// Pakai dengan: fmt.Errorf("%w: ...", errTransient)
var errTransient = errors.New("error sementara")

// handle memproses satu pesan sampai di-Ack/Nack, berdasarkan routing key asli
// dari producer (key konkret, mis. "app.v1.order.created", bukan pattern binding).
//
// Return error hanya jika publish ke retry queue gagal: channel kemungkinan
// bermasalah, jadi pemanggil harus reconnect. Pesan yang belum di-ack
// dikembalikan broker ke queue saat channel ditutup.
func handle(d amqp.Delivery, seen *dedup, r *retrier) error {
	key := originalRoutingKey(d)
	attempt := retryCount(d)

	// Redelivered=true: pesan pernah dikirim tapi belum sempat di-ack (mis. koneksi
	// putus di tengah proses). Handler sebaiknya idempotent.
	log.Printf("terima [routingKey=%s messageId=%s redelivered=%t retry=%d]: %s",
		key, d.MessageId, d.Redelivered, attempt, d.Body)

	// Dedup: duplikat dari producer (confirm hilang lalu kirim ulang) atau
	// redelivery broker (ack gagal karena koneksi putus) sudah pernah diproses.
	// Ack supaya keluar dari queue, tapi jangan diproses lagi.
	if d.MessageId != "" && seen.Seen(d.MessageId) {
		log.Printf("duplikat, dilewati: messageId=%s", d.MessageId)
		ack(d)
		return nil
	}
	if d.MessageId == "" {
		log.Println("peringatan: pesan tanpa MessageId, tidak bisa di-dedup")
	}

	var err error
	switch key {
	case "app.v1.order.created":
		err = handleOrderCreatedV1(d.Body, attempt)
	case "app.v2.order.created":
		err = handleOrderCreatedV2(d.Body, attempt)
	default:
		// Tidak ada handler (mis. producer sudah kirim versi baru, consumer belum
		// di-deploy): Nack tanpa requeue -> DLX -> DLQ. Tidak di-retry, karena
		// mencoba lagi tidak akan memunculkan handler. Setelah handler ditambah,
		// pesan bisa diputar ulang dari DLQ.
		log.Printf("routing key tidak dikenal, ke DLQ: %s", key)
		_ = d.Nack(false, false)
		return nil
	}

	if err == nil {
		// Tandai SEBELUM ack: kalau ack gagal (koneksi putus), broker mengirim ulang
		// pesan ini dan dedup akan melewatinya.
		if d.MessageId != "" {
			seen.Mark(d.MessageId)
		}
		ack(d)
		return nil
	}

	if errors.Is(err, errTransient) {
		if attempt < r.max {
			// Salin ke retry queue dulu (tunggu confirm), BARU ack aslinya. Kalau
			// dibalik dan koneksi putus di antaranya, pesan bisa hilang.
			if perr := r.Retry(d, key, attempt+1); perr != nil {
				return perr
			}
			log.Printf("gagal sementara, retry %d/%d dalam %s: %v", attempt+1, r.max, r.delay, err)
			ack(d)
			return nil
		}
		log.Printf("gagal sementara, retry habis (%d/%d), ke DLQ: %v", attempt, r.max, err)
	} else {
		// Error permanen (data tidak valid, dsb.): mencoba lagi tidak akan berhasil
		log.Printf("gagal permanen, ke DLQ: %v", err)
	}
	// Nack tanpa requeue -> DLX -> DLQ
	_ = d.Nack(false, false)
	return nil
}

func ack(d amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		log.Printf("ack: %v", err)
	}
}

func handleOrderCreatedV1(body []byte, attempt int) error {
	if len(body) == 0 {
		return errors.New("body kosong")
	}
	if err := simulate(body, attempt); err != nil {
		return err
	}
	log.Printf("[v1] order created: %s", body)
	return nil
}

func handleOrderCreatedV2(body []byte, attempt int) error {
	if len(body) == 0 {
		return errors.New("body kosong")
	}
	if err := simulate(body, attempt); err != nil {
		return err
	}
	log.Printf("[v2] order created: %s", body)
	return nil
}

// simulate memicu error buatan dari field "simulate" di body, supaya jalur
// retry dan DLQ bisa dicoba dari producer tanpa mengubah kode:
//
//	-body '{"id":1,"simulate":"transient"}'  selalu gagal sementara -> retry sampai habis -> DLQ
//	-body '{"id":1,"simulate":"flaky"}'      gagal sementara 2x, sukses di retry ke-2
//	-body '{"id":1,"simulate":"permanent"}'  gagal permanen -> langsung DLQ
//
// Hanya untuk belajar; hapus di kode produksi.
func simulate(body []byte, attempt int) error {
	var b struct {
		Simulate string `json:"simulate"`
	}
	_ = json.Unmarshal(body, &b) // body bukan JSON = tidak ada simulasi

	switch b.Simulate {
	case "transient":
		return fmt.Errorf("%w: simulasi DB down", errTransient)
	case "flaky":
		if attempt < 2 {
			return fmt.Errorf("%w: simulasi flaky (percobaan ke-%d)", errTransient, attempt+1)
		}
	case "permanent":
		return errors.New("simulasi data tidak valid")
	}
	return nil
}

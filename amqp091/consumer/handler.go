package main

import (
	"errors"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// handle memilih handler berdasarkan routing key asli dari producer
// (d.RoutingKey berisi key konkret, mis. "app.v1.order.created", bukan pattern binding).
func handle(d amqp.Delivery, seen *dedup) {
	// Redelivered=true: pesan pernah dikirim tapi belum sempat di-ack (mis. koneksi
	// putus di tengah proses). Handler sebaiknya idempotent.
	log.Printf("terima [routingKey=%s messageId=%s redelivered=%t]: %s",
		d.RoutingKey, d.MessageId, d.Redelivered, d.Body)

	// Dedup: duplikat dari producer (confirm hilang lalu kirim ulang) atau
	// redelivery broker (ack gagal karena koneksi putus) sudah pernah diproses.
	// Ack supaya keluar dari queue, tapi jangan diproses lagi.
	if d.MessageId != "" && seen.Seen(d.MessageId) {
		log.Printf("duplikat, dilewati: messageId=%s", d.MessageId)
		if err := d.Ack(false); err != nil {
			log.Printf("ack: %v", err)
		}
		return
	}
	if d.MessageId == "" {
		log.Println("peringatan: pesan tanpa MessageId, tidak bisa di-dedup")
	}

	var err error
	switch d.RoutingKey {
	case "app.v1.order.created":
		err = handleOrderCreatedV1(d.Body)
	case "app.v2.order.created":
		err = handleOrderCreatedV2(d.Body)
	default:
		// Tidak ada handler: Nack tanpa requeue supaya tidak berputar terus
		// (pesan dibuang, atau masuk DLX jika dikonfigurasi).
		log.Printf("routing key tidak dikenal: %s", d.RoutingKey)
		_ = d.Nack(false, false)
		return
	}

	if err != nil {
		// Gagal diproses -> Nack tanpa requeue. Untuk error sementara
		// (mis. DB down), pakai d.Nack(false, true) agar pesan dikirim ulang.
		log.Printf("gagal proses: %v", err)
		_ = d.Nack(false, false)
		return
	}

	// Tandai SEBELUM ack: kalau ack gagal (koneksi putus), broker mengirim ulang
	// pesan ini dan dedup akan melewatinya.
	if d.MessageId != "" {
		seen.Mark(d.MessageId)
	}
	if err := d.Ack(false); err != nil {
		log.Printf("ack: %v", err)
	}
}

func handleOrderCreatedV1(body []byte) error {
	if len(body) == 0 {
		return errors.New("body kosong")
	}
	log.Printf("[v1] order created: %s", body)
	return nil
}

func handleOrderCreatedV2(body []byte) error {
	if len(body) == 0 {
		return errors.New("body kosong")
	}
	log.Printf("[v2] order created: %s", body)
	return nil
}

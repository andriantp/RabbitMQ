package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const consumerTag = "amqp091-consumer"

// run menjalankan satu sesi: connect, setup topology, lalu consume sampai
// koneksi/channel putus (return error) atau ctx cancel (return nil).
// consumed=true jika sesi ini sempat sampai tahap consume.
func run(ctx context.Context, cfg config, seen *dedup) (consumed bool, err error) {
	// 1. Koneksi & channel
	conn, err := amqp.Dial(cfg.Host)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("channel: %w", err)
	}
	defer ch.Close()

	// Daftarkan NotifyClose SEBELUM operasi lain supaya tidak ada event yang terlewat.
	// Buffer 1 penting: library bisa deadlock jika channel notify tidak dibaca.
	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))

	// 2-6. Topology (DLX/DLQ, retry queue) & QoS; wajib diulang tiap reconnect karena channel baru
	if err := setup(ch, cfg); err != nil {
		return false, err
	}

	// 7. Publisher confirm: dipakai saat menyalin pesan ke retry queue, supaya
	//    pesan asli baru di-ack setelah salinannya pasti diterima broker.
	if err := ch.Confirm(false); err != nil {
		return false, fmt.Errorf("confirm mode: %w", err)
	}
	r := &retrier{ch: ch, queue: cfg.RetryQueue, max: cfg.RetryMax, delay: cfg.RetryDelay}

	// 8. Mulai consume dengan manual ack (autoAck=false).
	//    amqp091: ConsumeWithContext -> saat ctx cancel, consumer otomatis di-cancel
	//    di broker dan msgs ditutup (tidak perlu ch.Cancel manual).
	msgs, err := ch.ConsumeWithContext(ctx,
		cfg.Queue,   // queue
		consumerTag, // consumer tag
		false,       // autoAck
		false,       // exclusive
		false,       // noLocal (tidak didukung RabbitMQ)
		false,       // noWait
		nil,         // args
	)
	if err != nil {
		return false, fmt.Errorf("consume: %w", err)
	}

	log.Printf("terhubung, menunggu pesan: exchange=%q type=%s queue=%q bindingKey=%q dlq=%q retry=%q (maks %d, jeda %s)",
		cfg.Exchange, cfg.ExchangeType, cfg.Queue, cfg.BindingKey, cfg.DLQ, cfg.RetryQueue, cfg.RetryMax, cfg.RetryDelay)

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				// Ditutup karena ctx cancel (Ctrl+C) -> shutdown normal.
				// Pesan yang belum di-ack dikembalikan ke queue saat channel ditutup.
				if ctx.Err() != nil {
					log.Println("shutdown...")
					return true, nil
				}
				// Atau karena koneksi/channel putus, kadang sebelum NotifyClose terbaca
				return true, errors.New("delivery channel ditutup")
			}
			if err := handle(d, seen, r); err != nil {
				return true, err
			}

		case e := <-connClosed:
			return true, fmt.Errorf("koneksi tertutup: %v", e)

		case e := <-chClosed:
			// Channel bisa tertutup sendiri walau koneksi masih hidup (mis. error protokol)
			return true, fmt.Errorf("channel tertutup: %v", e)
		}
	}
}

// setup mendeklarasikan exchange, DLX/DLQ, queue, binding, dan QoS.
// Semua idempotent, jadi aman dipanggil ulang setiap reconnect.
//
//	producer ─▶ exchange ─[bindingKey]─▶ queue ──Nack(requeue=false)──▶ DLX ─[queue]─▶ DLQ
//	                                      ▲  │
//	                     TTL habis (""/queue)  └─gagal sementara (publish)─▶ retry queue
func setup(ch *amqp.Channel, cfg config) error {
	// 2. Declare exchange
	err := ch.ExchangeDeclare(
		cfg.Exchange,     // name
		cfg.ExchangeType, // kind: direct | topic | fanout | headers
		true,             // durable: tetap ada setelah broker restart
		false,            // autoDelete
		false,            // internal
		false,            // noWait
		nil,              // args
	)
	if err != nil {
		return fmt.Errorf("exchange declare: %w", err)
	}

	// 3. Dead letter: DLX (exchange, hanya meneruskan) + DLQ (queue, menyimpan).
	//    Harus ada SEBELUM pesan pertama di-Nack; kalau DLX tidak punya binding
	//    ke queue mana pun, pesan mati tetap hilang.
	if err := setupDeadLetter(ch, cfg); err != nil {
		return err
	}

	// 3b. Retry queue ber-TTL untuk error sementara (lihat retry.go)
	if err := setupRetry(ch, cfg); err != nil {
		return err
	}

	// 4. Declare queue utama, menunjuk ke DLX.
	//    Argumen queue tidak bisa diubah setelah dibuat: queue lama tanpa argumen
	//    ini akan ditolak broker dengan PRECONDITION_FAILED.
	_, err = ch.QueueDeclare(
		cfg.Queue, // name
		true,      // durable
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		amqp.Table{
			// Pesan yang di-Nack/Reject dengan requeue=false dikirim ke DLX ini
			"x-dead-letter-exchange": cfg.DLX,
			// ...dengan routing key = nama queue (bukan key asli dari producer), supaya
			// satu DLX bisa dipakai banyak queue, masing-masing ke DLQ-nya sendiri.
			// Key asli tetap tersimpan di header x-death ("routing-keys").
			"x-dead-letter-routing-key": cfg.Queue,
		},
	)
	if err != nil {
		return fmt.Errorf("queue declare: %w (queue %q sudah ada dengan argumen berbeda? hapus dulu di management UI)", err, cfg.Queue)
	}

	// 5. Binding: queue <- exchange via binding key
	err = ch.QueueBind(
		cfg.Queue,      // queue
		cfg.BindingKey, // binding key; untuk topic bisa pattern, mis. "app.#"
		cfg.Exchange,   // exchange
		false,          // noWait
		nil,            // args
	)
	if err != nil {
		return fmt.Errorf("queue bind: %w", err)
	}

	// 6. QoS/prefetch: broker hanya kirim 1 pesan yang belum di-ack ke consumer ini
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	return nil
}

// setupDeadLetter mendeklarasikan DLX, DLQ, dan binding di antaranya.
// DLQ sengaja tidak di-consume oleh aplikasi ini: isinya diperiksa manusia
// (management UI / rabbitmqadmin), lalu diputar ulang atau dihapus.
func setupDeadLetter(ch *amqp.Channel, cfg config) error {
	// DLX: exchange biasa. Tipe direct cukup karena routing key-nya = nama queue asal.
	err := ch.ExchangeDeclare(
		cfg.DLX,  // name
		"direct", // kind
		true,     // durable
		false,    // autoDelete
		false,    // internal
		false,    // noWait
		nil,      // args
	)
	if err != nil {
		return fmt.Errorf("dlx declare: %w", err)
	}

	// DLQ: queue biasa, durable supaya pesan mati bertahan saat broker restart
	_, err = ch.QueueDeclare(
		cfg.DLQ, // name
		true,    // durable
		false,   // autoDelete
		false,   // exclusive
		false,   // noWait
		nil,     // args
	)
	if err != nil {
		return fmt.Errorf("dlq declare: %w", err)
	}

	// DLQ <- DLX dengan key = nama queue asal (lihat x-dead-letter-routing-key)
	if err := ch.QueueBind(cfg.DLQ, cfg.Queue, cfg.DLX, false, nil); err != nil {
		return fmt.Errorf("dlq bind: %w", err)
	}
	return nil
}

// setupRetry mendeklarasikan retry queue: tanpa consumer, pesan kedaluwarsa
// setelah RetryDelay lalu dead-letter langsung kembali ke queue utama.
func setupRetry(ch *amqp.Channel, cfg config) error {
	_, err := ch.QueueDeclare(
		cfg.RetryQueue, // name
		true,           // durable
		false,          // autoDelete
		false,          // exclusive
		false,          // noWait
		amqp.Table{
			// Jeda sebelum dicoba lagi. TTL per queue (bukan per pesan): RabbitMQ hanya
			// meng-expire pesan di kepala queue, jadi TTL per pesan yang berbeda-beda
			// di satu queue bisa saling menahan.
			"x-message-ttl": cfg.RetryDelay.Milliseconds(),
			// Default exchange ("") + routing key = nama queue utama: kembali HANYA ke
			// queue ini, tidak ke queue lain yang bind ke exchange utama.
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": cfg.Queue,
		},
	)
	if err != nil {
		return fmt.Errorf("retry queue declare: %w (RABBITMQ_RETRY_DELAY berubah? hapus queue %q dulu)", err, cfg.RetryQueue)
	}
	return nil
}

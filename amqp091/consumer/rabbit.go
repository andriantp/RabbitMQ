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

	// 2-5. Topology & QoS; wajib diulang tiap reconnect karena channel baru
	if err := setup(ch, cfg); err != nil {
		return false, err
	}

	// 6. Mulai consume dengan manual ack (autoAck=false).
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

	log.Printf("terhubung, menunggu pesan: exchange=%q type=%s queue=%q bindingKey=%q",
		cfg.Exchange, cfg.ExchangeType, cfg.Queue, cfg.BindingKey)

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
			handle(d, seen)

		case e := <-connClosed:
			return true, fmt.Errorf("koneksi tertutup: %v", e)

		case e := <-chClosed:
			// Channel bisa tertutup sendiri walau koneksi masih hidup (mis. error protokol)
			return true, fmt.Errorf("channel tertutup: %v", e)
		}
	}
}

// setup mendeklarasikan exchange, queue, binding, dan QoS.
// Semua idempotent, jadi aman dipanggil ulang setiap reconnect.
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

	// 3. Declare queue
	_, err = ch.QueueDeclare(
		cfg.Queue, // name
		true,      // durable
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		nil,       // args
	)
	if err != nil {
		return fmt.Errorf("queue declare: %w", err)
	}

	// 4. Binding: queue <- exchange via binding key
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

	// 5. QoS/prefetch: broker hanya kirim 1 pesan yang belum di-ack ke consumer ini
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	return nil
}

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const confirmTimeout = 5 * time.Second

var (
	errUnroutable = errors.New("di-ack broker, tapi pesan tidak masuk queue mana pun")
	errNack       = errors.New("broker NACK pesan")
)

// publisher membungkus koneksi + channel dalam mode publisher confirm.
type publisher struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	exchange string
	returns  chan amqp.Return

	// Terisi saat koneksi/channel putus; dipakai untuk memicu reconnect
	connClosed chan *amqp.Error
	chClosed   chan *amqp.Error
}

// newPublisher: connect, declare exchange, dan aktifkan publisher confirm.
func newPublisher(cfg config) (*publisher, error) {
	// 1. Koneksi & channel
	conn, err := amqp.Dial(cfg.Host)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("channel: %w", err)
	}

	p := &publisher{conn: conn, ch: ch, exchange: cfg.Exchange}

	// Daftarkan NotifyClose sedini mungkin. Buffer 1: library bisa deadlock
	// jika channel notify tidak dibaca.
	p.connClosed = conn.NotifyClose(make(chan *amqp.Error, 1))
	p.chClosed = ch.NotifyClose(make(chan *amqp.Error, 1))

	// 2. Declare exchange (harus sama persis dengan consumer: name, type, durable, ...)
	//    Producer tidak declare queue/binding; itu urusan consumer.
	err = ch.ExchangeDeclare(
		cfg.Exchange,     // name
		cfg.ExchangeType, // kind
		true,             // durable
		false,            // autoDelete
		false,            // internal
		false,            // noWait
		nil,              // args
	)
	if err != nil {
		p.Close()
		return nil, fmt.Errorf("exchange declare: %w", err)
	}

	// 3. Publisher confirm. amqp091: tidak perlu NotifyPublish; tiap publish
	//    mengembalikan DeferredConfirmation miliknya sendiri.
	if err := ch.Confirm(false); err != nil {
		p.Close()
		return nil, fmt.Errorf("confirm mode: %w", err)
	}

	// mandatory=true: kalau tidak ada queue yang cocok dengan routing key,
	// broker mengembalikan pesan lewat NotifyReturn (bukan dibuang diam-diam)
	p.returns = ch.NotifyReturn(make(chan amqp.Return, 1))

	return p, nil
}

func (p *publisher) Close() {
	p.ch.Close()
	p.conn.Close()
}

// Publish mengirim satu pesan lalu menunggu confirm dari broker.
func (p *publisher) Publish(ctx context.Context, routingKey string, msg message) error {
	// WithoutCancel: pesan yang sedang dikirim tetap ditunggu confirm-nya walau
	// Ctrl+C ditekan; batasnya hanya confirmTimeout.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), confirmTimeout)
	defer cancel()

	// 4. Publish (amqp091: dengan context + deferred confirm)
	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx,
		p.exchange, // exchange
		routingKey, // routing key konkret, mis. "app.v1.order.created"
		true,       // mandatory
		false,      // immediate (tidak didukung RabbitMQ 3+)
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // simpan ke disk (butuh queue durable)
			Timestamp:    time.Now(),
			MessageId:    msg.id, // dipakai consumer untuk dedup
			Body:         msg.body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return p.waitConfirm(ctx, dc)
}

// waitConfirm menunggu confirm untuk pesan ini saja (dc), bukan dari channel bersama.
func (p *publisher) waitConfirm(ctx context.Context, dc *amqp.DeferredConfirmation) error {
	select {
	case <-dc.Done():
		if !dc.Acked() {
			return fmt.Errorf("%w (deliveryTag=%d)", errNack, dc.DeliveryTag)
		}
		// Jika unroutable, broker mengirim basic.return SEBELUM ack, jadi pada
		// titik ini return (kalau ada) sudah ada di buffer.
		select {
		case r := <-p.returns:
			log.Printf("pesan dikembalikan (tidak ada queue yang cocok): routingKey=%s reply=%d %s",
				r.RoutingKey, r.ReplyCode, r.ReplyText)
			return errUnroutable
		default:
			return nil
		}

	case e := <-p.connClosed:
		// Koneksi putus sebelum confirm datang: status pesan tidak diketahui
		return fmt.Errorf("koneksi tertutup sebelum confirm: %v", e)

	case e := <-p.chClosed:
		return fmt.Errorf("channel tertutup sebelum confirm: %v", e)

	case <-ctx.Done():
		return fmt.Errorf("menunggu confirm: %w", ctx.Err())
	}
}

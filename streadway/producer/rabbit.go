package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
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
	confirms chan amqp.Confirmation
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

	// Daftarkan NotifyClose sedini mungkin. Buffer 1: streadway bisa deadlock
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

	// 3. Publisher confirm: broker membalas ack/nack untuk setiap pesan
	if err := ch.Confirm(false); err != nil {
		p.Close()
		return nil, fmt.Errorf("confirm mode: %w", err)
	}
	p.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))

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
func (p *publisher) Publish(routingKey string, msg message) error {
	// 4. Publish (streadway: tanpa context)
	err := p.ch.Publish(
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

	return p.waitConfirm()
}

// waitConfirm menunggu ack/nack. Jika unroutable, broker kirim basic.return dulu, lalu ack.
func (p *publisher) waitConfirm() error {
	timeout := time.After(confirmTimeout)
	returned := false
	for {
		select {
		case r := <-p.returns:
			logReturn(r)
			returned = true // lanjut menunggu confirm

		case c := <-p.confirms:
			if !c.Ack {
				return fmt.Errorf("%w (deliveryTag=%d)", errNack, c.DeliveryTag)
			}
			// return & ack bisa sudah sama-sama ada di buffer, dan select memilih acak;
			// cek returns sekali lagi tanpa blocking supaya return tidak terlewat.
			select {
			case r := <-p.returns:
				logReturn(r)
				returned = true
			default:
			}
			if returned {
				return errUnroutable
			}
			return nil

		case e := <-p.connClosed:
			// Koneksi putus sebelum confirm datang: status pesan tidak diketahui
			return fmt.Errorf("koneksi tertutup sebelum confirm: %v", e)

		case e := <-p.chClosed:
			return fmt.Errorf("channel tertutup sebelum confirm: %v", e)

		case <-timeout:
			return errors.New("timeout menunggu confirm dari broker")
		}
	}
}

func logReturn(r amqp.Return) {
	log.Printf("pesan dikembalikan (tidak ada queue yang cocok): routingKey=%s reply=%d %s",
		r.RoutingKey, r.ReplyCode, r.ReplyText)
}

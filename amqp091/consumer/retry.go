package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Retry queue ber-TTL:
//
//	queue ──gagal sementara──▶ (publish) retry queue ──TTL habis──▶ (dead letter) queue
//	                                     tanpa consumer
//
// Retry queue tidak punya consumer. x-message-ttl membuat pesan kedaluwarsa,
// lalu dead-letter lewat default exchange ("") LANGSUNG ke queue asal, bukan
// lewat exchange utama: queue lain yang bind ke exchange yang sama tidak ikut
// menerima ulang pesan ini.
//
// Karena pesan kembali lewat default exchange, d.RoutingKey berubah menjadi
// nama queue. Key asli dibawa di header x-original-routing-key.

const (
	headerRetryCount  = "x-retry-count"          // sudah berapa kali di-retry
	headerOriginalKey = "x-original-routing-key" // routing key asli dari producer

	confirmTimeout = 5 * time.Second
)

type retrier struct {
	ch    *amqp.Channel // channel consumer, dalam mode publisher confirm
	queue string        // nama retry queue
	max   int           // maksimal retry sebelum ke DLQ
	delay time.Duration // TTL retry queue (hanya untuk log)
}

// Retry menyalin pesan ke retry queue dan menunggu confirm broker.
// Pemanggil baru boleh Ack pesan asli setelah Retry sukses.
func (r *retrier) Retry(d amqp.Delivery, key string, attempt int) error {
	ctx, cancel := context.WithTimeout(context.Background(), confirmTimeout)
	defer cancel()

	// amqp091: confirm per pesan lewat DeferredConfirmation
	dc, err := r.ch.PublishWithDeferredConfirmWithContext(ctx,
		"",      // default exchange: routing key = nama queue tujuan
		r.queue, // retry queue
		false,   // mandatory
		false,   // immediate
		retryPublishing(d, key, attempt),
	)
	if err != nil {
		return fmt.Errorf("publish retry: %w", err)
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("menunggu confirm retry: %w", err)
	}
	if !acked {
		return errors.New("broker NACK pesan retry")
	}
	return nil
}

// retryPublishing menyalin pesan dengan header retry yang diperbarui.
// MessageId tetap sama supaya dedup tetap bekerja lintas retry.
func retryPublishing(d amqp.Delivery, key string, attempt int) amqp.Publishing {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		// x-death dan sejenisnya dikelola broker; jangan dikirim ulang dari client
		if k == "x-death" || strings.HasPrefix(k, "x-first-death-") || strings.HasPrefix(k, "x-last-death-") {
			continue
		}
		headers[k] = v
	}
	headers[headerRetryCount] = int32(attempt)
	headers[headerOriginalKey] = key

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    amqp.Persistent,
		CorrelationId:   d.CorrelationId,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body,
	}
}

// retryCount membaca header x-retry-count (0 jika belum pernah di-retry).
// Tipe integer di header bisa berbeda tergantung pengirim, jadi cek beberapa.
func retryCount(d amqp.Delivery) int {
	switch v := d.Headers[headerRetryCount].(type) {
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}

// originalRoutingKey: key asli dari producer, walau pesan sudah melewati retry queue.
func originalRoutingKey(d amqp.Delivery) string {
	if k, ok := d.Headers[headerOriginalKey].(string); ok && k != "" {
		return k
	}
	return d.RoutingKey
}

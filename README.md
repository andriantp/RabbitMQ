# RabbitMQ Go: streadway/amqp vs amqp091-go

Producer dan consumer yang sama, ditulis dengan dua library supaya bisa dibandingkan berdampingan:

| Folder | Library | Status |
|---|---|---|
| [`streadway/`](streadway/) | `github.com/streadway/amqp` v1.1.0 | Lama, deprecated, tidak dirawat |
| [`amqp091/`](amqp091/) | `github.com/rabbitmq/amqp091-go` v1.17.0 | Penerus resmi, dirawat tim RabbitMQ |

Keduanya memakai protokol AMQP 0-9-1, jadi bisa saling bicara: producer `streadway` bisa dikonsumsi consumer `amqp091`, dan sebaliknya. Karena itu migrasi bisa dilakukan bertahap per service.

## Struktur

```
.
├── .env.example
├── reconnect/                  # modul bersama: loop reconnect (backoff, max retries)
│   ├── reconnect.go            # reconnect.Run(ctx, maxRetries, session)
│   └── reconnect_test.go
├── streadway/                  # modul Go sendiri (go.mod, replace -> ../reconnect)
│   ├── consumer/
│   │   ├── main.go             # signal ctx + reconnect.Run(run)
│   │   ├── config.go           # baca env
│   │   ├── rabbit.go           # run(): satu sesi koneksi; setup(): exchange/queue/bind/QoS
│   │   ├── handler.go          # switch routing key -> handler per versi, ack/nack
│   │   ├── retry.go            # retry queue ber-TTL: salin pesan + header retry, tunggu confirm
│   │   └── dedup.go            # ingat MessageId yang sudah diproses
│   └── producer/
│       ├── main.go             # flag + signal ctx + reconnect.Run(run)
│       ├── config.go           # baca env
│       ├── producer.go         # publish periodik, pending, MessageId
│       └── rabbit.go           # publisher: connect, confirm, publish, tunggu confirm
└── amqp091/                    # struktur sama persis
```

Satu folder adalah satu `package main` yang terdiri dari beberapa file. Jalankan dengan `go run ./consumer`, bukan `go run ./consumer/main.go`.

Loop reconnect tidak bergantung pada library AMQP, jadi keempat `main.go` memakai satu implementasi di [`reconnect/`](reconnect/). Modul `streadway` dan `amqp091` menunjuk ke sana lewat `replace example.com/rabbitmq/reconnect => ../reconnect` di `go.mod`. Test-nya: `cd reconnect && go test ./...`.

## Konfigurasi

```bash
cp .env.example .env   # sesuaikan kredensial di RABBITMQ_HOST
source .env
```

| Env | Dipakai | Contoh | Keterangan |
|---|---|---|---|
| `RABBITMQ_HOST` | keduanya | `amqp://guest:guest@localhost:5672/` | URL lengkap, bukan hanya hostname |
| `RABBITMQ_EXCHANGE` | keduanya | `app` | Nama exchange |
| `RABBITMQ_EXCHANGE_TYPE` | keduanya | `topic` | `direct` / `topic` / `fanout` / `headers`, tanpa prefix |
| `RABBITMQ_QUEUE` | consumer | `app-test` | Nama queue |
| `RABBITMQ_BINDING_KEY` | consumer | `app.#` | Pattern untuk `QueueBind` |
| `RABBITMQ_ROUTING_KEY` | producer | `app.v1.order.created` | Key konkret untuk publish. Hanya default untuk flag `-key`; boleh kosong jika `-key` diisi |
| `RABBITMQ_MAX_RETRIES` | keduanya | `5` | Opsional, default `5`. Batas percobaan **reconnect** |
| `RABBITMQ_RETRY_MAX` | consumer | `3` | Opsional, default `3`. Batas **retry pesan** yang gagal sementara sebelum ke DLQ |
| `RABBITMQ_RETRY_DELAY` | consumer | `10s` | Opsional, default `10s`. Jeda antar retry (TTL retry queue), format durasi Go |

## Menjalankan

Pastikan RabbitMQ sudah berjalan, lalu:

```bash
source .env
cd streadway        # atau: cd amqp091

go run ./consumer                                              # terminal 1
go run ./producer                                              # terminal 2: kirim app.v1.order.created tiap 2s
go run ./producer -key app.v2.order.created -body '{"id":2}'   # masuk case v2
go run ./producer -key app.v1.payment.paid                     # cocok dengan app.#, tapi tidak ada case -> Nack -> DLQ
go run ./producer -key other.v1.x                              # tidak cocok binding mana pun -> 312 NO_ROUTE, dibuang
go run ./producer -interval 500ms                              # lebih cepat
```

Mencoba retry dan DLQ (field `simulate` di body dibaca handler, lihat `simulate()` di [`handler.go`](amqp091/consumer/handler.go)):

```bash
go run ./producer -body '{"id":1,"simulate":"flaky"}'       # gagal sementara 2x, sukses di retry ke-2
go run ./producer -body '{"id":1,"simulate":"transient"}'   # retry 3x lalu ke DLQ
go run ./producer -body '{"id":1,"simulate":"permanent"}'   # langsung ke DLQ, tanpa retry
```

Flag producer:

| Flag | Default | Keterangan |
|---|---|---|
| `-key` | `$RABBITMQ_ROUTING_KEY` | Routing key; wajib ada dari salah satunya |
| `-body` | JSON contoh `{"id":N,"created_at":...}` | Isi pesan tetap |
| `-interval` | `2s` | Jeda antar publish, harus > 0 |

Producer dan consumer sama-sama berjalan sampai Ctrl+C.

## Routing: topic exchange + versioning

```
producer ──(app.v1.order.created)──▶ exchange "app" (topic)
                                        │ binding: app.#
                                        ▼
                                   queue "app-test" ──▶ consumer ──▶ switch d.RoutingKey
                                        │                            ├─ app.v1.order.created -> handleOrderCreatedV1
                                        │                            ├─ app.v2.order.created -> handleOrderCreatedV2
                                        │                            ├─ error sementara      -> retry queue (lihat Retry)
                                        │                            └─ default / error lain -> Nack(requeue=false)
                                        │ dead letter                                                  │
                                        ▼                                                              │
                         DLX "app.dlx" (direct) ◀──────────────────────────────────────────────────────┘
                                        │ binding: app-test
                                        ▼
                         DLQ "app-test.dlq"   (disimpan, tidak di-consume)
```

- Di topic exchange, pemisah segmen adalah **titik**. `app/v1/x` dianggap satu kata, sehingga wildcard tidak bisa memecahnya.
- `*` cocok dengan tepat satu segmen, sedangkan `#` cocok dengan nol atau lebih segmen. `app.#` menerima semua event `app`.
- Consumer bind dengan **pattern** (`RABBITMQ_BINDING_KEY`), sedangkan producer publish dengan **key konkret** (`RABBITMQ_ROUTING_KEY`). Switch di [`handler.go`](amqp091/consumer/handler.go) membaca `d.RoutingKey`, yaitu key asli dari producer.
- Untuk menambah event atau versi baru, tambahkan `case` di `handler.go`. Event yang tidak punya `case` akan di-Nack tanpa requeue dan masuk DLQ (lihat [Dead letter](#dead-letter-dlx--dlq)).
- `amq.topic` yang terlihat di UI adalah **nama** exchange bawaan, bukan tipe. Prefix `amq.` dicadangkan oleh RabbitMQ.

## Fitur

### Consumer

- **Manual ack + QoS** `Qos(1, 0, false)`: maksimal 1 pesan belum di-ack per consumer. Beberapa consumer di queue yang sama akan berbagi pesan secara round-robin.
- **Auto-reconnect**: `NotifyClose` pada connection dan channel memicu reconnect dari nol (dial, channel, declare, bind, QoS, consume). Topology wajib dibuat ulang karena channel-nya baru.
- **Exponential backoff** 1s → 2s → 4s … maksimal 30s, **maksimal `RABBITMQ_MAX_RETRIES` percobaan gagal berturut-turut**, lalu `exit 1` supaya systemd, Docker, atau Kubernetes yang me-restart. Hitungan di-reset hanya jika sesi berhasil consume **dan bertahan minimal 10s** (`reconnect.StableAfter`); sesi yang langsung putus lagi tetap dihitung gagal, jadi error yang selalu terulang berakhir dengan `exit 1`, bukan reconnect selamanya.
- **Dedup `MessageId`**: pesan dengan `MessageId` yang sudah diproses akan di-Ack lalu dilewati. ID ditandai setelah handler sukses dan sebelum Ack.
- **Retry queue ber-TTL**: error sementara dicoba lagi setelah `RABBITMQ_RETRY_DELAY`, maksimal `RABBITMQ_RETRY_MAX` kali, baru ke DLQ.
- **Dead letter (DLX + DLQ)**: pesan yang di-Nack tanpa requeue (routing key tidak dikenal, error permanen, retry habis) disimpan di DLQ, bukan dibuang.
- **Graceful shutdown**: Ctrl+C membatalkan consumer di broker, dan pesan yang belum di-ack dikembalikan ke queue.

### Producer

- **Publish periodik** sesuai `-interval`.
- **Publisher confirm**: setiap pesan ditunggu ack dari broker, maksimal 5s.
- **`mandatory=true`**: pesan yang tidak masuk queue mana pun (`312 NO_ROUTE`) dilaporkan, tidak hilang tanpa jejak.
- **Auto-reconnect + backoff + max retries**, sama seperti consumer.
- **Pending**: pesan yang gagal karena koneksi putus disimpan, lalu dikirim ulang segera setelah reconnect.
- **`MessageId` stabil**: ID acak 128-bit dibuat sekali per pesan dan tetap sama saat pesan dikirim ulang.

| Kondisi | Tindakan producer |
|---|---|
| Koneksi putus saat idle | Reconnect, tidak ada pesan hilang |
| Koneksi putus saat menunggu confirm | Simpan sebagai pending, reconnect, kirim ulang |
| Unroutable / NACK | Log lalu dibuang; broker sehat, jadi tidak reconnect |

### Jaminan pengiriman: at-least-once

Kalau koneksi putus **setelah** pesan sampai ke broker tapi **sebelum** confirm diterima, producer tidak bisa tahu apakah pesannya sudah sampai, sehingga ia mengirim ulang. Akibatnya bisa muncul **duplikat**, dan itu yang ditangani oleh dedup `MessageId` di consumer.

Perilaku ini sudah diuji dengan proxy yang memutus koneksi tepat saat frame `basic.publish` lewat:

| Skenario | Tanpa dedup | Dengan dedup |
|---|---|---|
| Pesan sampai, confirm hilang | `1 2 3 3 4 …` (dobel) | `1 2 3 4 …` + log `duplikat, dilewati` |
| Pesan tidak sampai | `… 6 7 8 …` (dikirim ulang, tidak hilang) | sama |

`Redelivered=true` hanya menandai pengiriman ulang **dari broker ke consumer** (ack gagal). Duplikat yang berasal dari producer tetap tiba dengan `Redelivered=false`.

## Dead letter (DLX + DLQ)

Consumer mendeklarasikan sendiri di [`setup()`](amqp091/consumer/rabbit.go), tanpa env baru:

| | Nama | Keterangan |
|---|---|---|
| DLX | `<RABBITMQ_EXCHANGE>.dlx` (`app.dlx`) | Exchange `direct`, hanya meneruskan, tidak menyimpan |
| DLQ | `<RABBITMQ_QUEUE>.dlq` (`app-test.dlq`) | Queue durable, tempat pesan mati menumpuk |

Queue utama dibuat dengan dua argumen:

- `x-dead-letter-exchange = app.dlx`: ke mana pesan mati dikirim.
- `x-dead-letter-routing-key = app-test`: routing key diganti nama queue, supaya satu DLX bisa dipakai banyak queue dan masing-masing masuk ke DLQ-nya sendiri. Key asli dari producer (`app.v1.payment.paid`) tetap tersimpan di header `x-death`, bersama `reason`, `queue`, dan `count`.

| Kejadian | Masuk DLQ? |
|---|---|
| Routing key tidak punya `case` | Ya |
| Handler return error permanen | Ya, langsung |
| Handler return error sementara (`errTransient`) | Ya, setelah `RABBITMQ_RETRY_MAX` kali retry |
| Duplikat `MessageId` (di-Ack) | Tidak |
| `Nack(requeue=true)` | Tidak, kembali ke queue utama |
| Unroutable dari producer (`312 NO_ROUTE`) | Tidak, pesan tidak pernah masuk queue. Untuk ini pakai *alternate exchange* |

Melihat isi DLQ tanpa menghapusnya:

```bash
rabbitmqadmin -u guest -p guest get queue=app-test.dlq count=10 ackmode=ack_requeue_true
```

Atau lewat management UI: **Queues → app-test.dlq → Get messages**. Untuk memutar ulang, pindahkan pesan ke exchange `app` dengan routing key aslinya (UI: plugin *shovel*, atau ambil lalu publish ulang).

> **Upgrade dari versi tanpa DLX:** argumen queue tidak bisa diubah, jadi queue `app-test` yang sudah ada akan ditolak dengan `PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange'`. Hapus queue lama sekali di management UI (pastikan kosong), lalu jalankan consumer lagi.

## Retry queue ber-TTL

Untuk error sementara (DB down, timeout) yang mungkin pulih sendiri. `Nack(requeue=true)` tidak dipakai karena pesan langsung kembali ke depan queue dan gagal lagi tanpa jeda.

```
                 ┌─────────── TTL habis (RABBITMQ_RETRY_DELAY) ───────────┐
                 ▼              dead letter via default exchange ""       │
queue "app-test" ──▶ consumer ── gagal sementara ──▶ queue "app-test.retry"
                        │          (publish + confirm, lalu Ack)   tanpa consumer
                        └─ retry habis / error permanen ──▶ Nack ──▶ DLX ──▶ DLQ
```

1. Handler mengembalikan error yang dibungkus `errTransient` (`fmt.Errorf("%w: ...", errTransient)`). Error lain dianggap permanen.
2. Consumer **menyalin** pesan ke `app-test.retry` lewat default exchange, menunggu publisher confirm, **lalu** Ack pesan asli. Urutan ini penting: kalau Ack duluan lalu koneksi putus, pesan hilang. Kalau Ack gagal setelah salinan terkirim, duplikatnya ditangani dedup `MessageId`, yang ikut disalin.
3. `app-test.retry` tidak punya consumer. Setelah `x-message-ttl` habis, pesan dead-letter ke default exchange dengan routing key `app-test`, yaitu **langsung kembali ke queue ini saja**, tidak ke queue lain yang bind ke exchange `app`.
4. Dua header dibawa di setiap salinan:

| Header | Isi | Kenapa perlu |
|---|---|---|
| `x-retry-count` | 1, 2, 3, … | Hitungan retry. Header `x-death` dari broker tidak dipakai karena di-reset setiap pesan disalin ulang oleh client |
| `x-original-routing-key` | `app.v1.order.created` | Pesan kembali lewat default exchange, jadi `d.RoutingKey` berubah menjadi `app-test`. Handler memakai header ini untuk `switch` |

5. Setelah `RABBITMQ_RETRY_MAX` kali, pesan di-Nack ke DLQ dengan header di atas tetap terbawa, jadi di DLQ terlihat berapa kali sudah dicoba.

Error sementara di retry ke-2 yang lalu sukses di retry ke-3 akan di-Ack normal dan tidak pernah sampai ke DLQ.

Kalau publish ke retry queue gagal (timeout, NACK, channel putus), `handle()` mengembalikan error dan consumer reconnect. Pesan asli yang belum di-ack dikembalikan broker ke queue.

Di streadway hal ini wajib, karena confirm yang terlambat bisa tertinggal di channel `NotifyPublish` lalu terbaca oleh publish berikutnya. Di amqp091 confirm per pesan (`DeferredConfirmation`), jadi masalah itu tidak ada.

## Perbedaan kode: streadway vs amqp091

Exchange, queue, binding, QoS, ack/nack, `NotifyClose`, dan `NotifyReturn` memiliki API **yang sama persis**. Perbedaannya hanya ini:

| | `streadway` | `amqp091` |
|---|---|---|
| Import | `"github.com/streadway/amqp"` | `amqp "github.com/rabbitmq/amqp091-go"` |
| Consume | `ch.Consume(...)` | `ch.ConsumeWithContext(ctx, ...)` |
| Shutdown consumer | `case <-ctx.Done()` lalu `ch.Cancel(tag)` manual | ctx cancel otomatis membatalkan consumer dan menutup `msgs` |
| Publish | `ch.Publish(...)`, tanpa context; deprecated di amqp091 | `ch.PublishWithDeferredConfirmWithContext(ctx, ...)` |
| Confirm | `NotifyPublish`: satu channel untuk semua pesan | `*DeferredConfirmation` per pesan (`dc.Done()`, `dc.Acked()`) |
| Timeout confirm | `time.After` | `context.WithTimeout` |
| Auto-reconnect | Tidak ada | **Tidak ada juga**, tetap harus ditulis sendiri |

Untuk migrasi, cukup ganti import dengan alias `amqp`, lalu pindahkan `Publish` ke versi `...WithContext`.

## Batasan

- **Dedup in-memory**: hilang saat consumer restart dan tidak dibagi antar instance. Kalau ada beberapa consumer di queue yang sama, pesan asli dan duplikatnya bisa jatuh ke consumer berbeda. Untuk produksi, pakai penyimpanan bersama, misalnya Redis `SET <id> 1 NX EX 600` atau unique constraint di database.
- **Jeda retry tetap**: satu retry queue berarti satu TTL untuk semua percobaan, belum exponential. Untuk backoff bertingkat, pakai beberapa retry queue (`retry.5s`, `retry.30s`, …) dan pilih berdasarkan `x-retry-count`.
- **Mengubah `RABBITMQ_RETRY_DELAY`** setelah retry queue dibuat akan error `PRECONDITION_FAILED` karena `x-message-ttl` adalah argumen queue. Hapus `<queue>.retry` dulu.
- **Urutan tidak dijaga**: pesan yang di-retry diproses setelah pesan yang datang belakangan.
- **Memutar ulang dari DLQ masih manual.**
- **DLQ tidak dibatasi**: tanpa `x-max-length` atau `x-message-ttl`, DLQ bisa terus tumbuh kalau tidak dipantau.
- Mengubah tipe exchange yang sudah ada, misalnya `direct` ke `topic` dengan nama yang sama, akan error `PRECONDITION_FAILED`. Pakai nama baru, atau hapus exchange lama di management UI.

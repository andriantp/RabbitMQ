package reconnect

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errDown = errors.New("down")

// fast: backoff 1ms, sesi sehat langsung dianggap stabil
var fast = policy{time.Millisecond, time.Millisecond, 0}

func TestGiveUpAfterMaxRetries(t *testing.T) {
	calls := 0
	err := run(context.Background(), 3, fast,
		func(context.Context) (bool, error) { calls++; return false, errDown })
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if calls != 4 { // 1 percobaan awal + 3 retry
		t.Fatalf("calls = %d, want 4", calls)
	}
}

func TestHealthyResetsRetries(t *testing.T) {
	// maxRetries=2. Pola: gagal, gagal, sehat lalu putus (reset), gagal, gagal -> menyerah.
	// Tanpa reset, sudah menyerah di panggilan ke-3.
	healthy := []bool{false, false, true, false, false}
	calls := 0
	err := run(context.Background(), 2, fast,
		func(context.Context) (bool, error) {
			h := healthy[calls]
			calls++
			return h, errDown
		})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if calls != len(healthy) {
		t.Fatalf("calls = %d, want %d", calls, len(healthy))
	}
}

func TestCancelReturnsNil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := run(ctx, 5, fast,
		func(context.Context) (bool, error) { cancel(); return true, nil })
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := run(ctx, 5, policy{time.Hour, time.Hour, 0},
		func(context.Context) (bool, error) { return false, errDown })
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("backoff tidak terputus oleh ctx cancel")
	}
}

func TestShortHealthySessionDoesNotReset(t *testing.T) {
	// Sesi selalu "sehat" tapi langsung putus (lebih singkat dari stableAfter):
	// harus tetap menyerah, bukan reconnect selamanya.
	calls := 0
	err := run(context.Background(), 3, policy{time.Millisecond, time.Millisecond, time.Hour},
		func(context.Context) (bool, error) {
			calls++
			if calls > 100 {
				t.Fatal("tidak pernah menyerah")
			}
			return true, errDown
		})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if calls != 4 {
		t.Fatalf("calls = %d, want 4", calls)
	}
}

func TestLongHealthySessionResets(t *testing.T) {
	// Sesi sehat yang bertahan >= stableAfter me-reset hitungan retry.
	// maxRetries=1: gagal (retry 1/1), sehat lama (reset, retry 1/1), gagal -> menyerah
	// di panggilan ke-3. Tanpa reset, sudah menyerah di panggilan ke-2.
	plan := []struct {
		healthy bool
		sleep   time.Duration
	}{{false, 0}, {true, 20 * time.Millisecond}, {false, 0}}
	calls := 0
	err := run(context.Background(), 1, policy{time.Millisecond, time.Millisecond, 10 * time.Millisecond},
		func(context.Context) (bool, error) {
			s := plan[calls]
			calls++
			time.Sleep(s.sleep)
			return s.healthy, errDown
		})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if calls != len(plan) {
		t.Fatalf("calls = %d, want %d", calls, len(plan))
	}
}

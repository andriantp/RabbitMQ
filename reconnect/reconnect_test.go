package reconnect

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errDown = errors.New("down")

func TestGiveUpAfterMaxRetries(t *testing.T) {
	calls := 0
	err := run(context.Background(), 3, time.Millisecond, time.Millisecond,
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
	err := run(context.Background(), 2, time.Millisecond, time.Millisecond,
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
	err := run(ctx, 5, time.Millisecond, time.Millisecond,
		func(context.Context) (bool, error) { cancel(); return true, nil })
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := run(ctx, 5, time.Hour, time.Hour,
		func(context.Context) (bool, error) { return false, errDown })
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("backoff tidak terputus oleh ctx cancel")
	}
}

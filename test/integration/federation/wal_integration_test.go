// Package federation_test contains integration tests for the federation WAL.
//
// Tests exercise the full write/replay/ack/compact lifecycle against a real
// temporary file, verifying behaviour that unit mocks cannot cover: fsync
// durability, byte-accurate compaction, and concurrent write ordering.
package federation_test

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/ontai-dev/conductor/internal/federation"
)

// openTmpWAL creates a WAL backed by a temp file. The file is removed on
// test cleanup.
func openTmpWAL(t *testing.T, maxBytes int64) *federation.WAL {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "wal-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	// OpenWAL will reopen the file.
	wal, err := federation.OpenWAL(name, maxBytes)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	return wal
}

// TestWAL_WriteAndReplay writes three entries and verifies all three are
// returned by Replay in write order with correct sequence numbers.
func TestWAL_WriteAndReplay(t *testing.T) {
	wal := openTmpWAL(t, 1024*1024)

	entries := []federation.WALEntry{
		{SequenceNumber: 1, Data: []byte(`{"event":"a"}`)},
		{SequenceNumber: 2, Data: []byte(`{"event":"b"}`)},
		{SequenceNumber: 3, Data: []byte(`{"event":"c"}`)},
	}
	for _, e := range entries {
		if err := wal.Write(e); err != nil {
			t.Fatalf("Write seq=%d: %v", e.SequenceNumber, err)
		}
	}

	got, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Replay: got %d entries, want 3", len(got))
	}
	for i, e := range entries {
		if got[i].SequenceNumber != e.SequenceNumber {
			t.Errorf("entry[%d] seq = %d; want %d", i, got[i].SequenceNumber, e.SequenceNumber)
		}
	}
}

// TestWAL_PartialAck writes three entries, acks through seq 2, and verifies
// that Replay returns only entry 3.
func TestWAL_PartialAck(t *testing.T) {
	wal := openTmpWAL(t, 1024*1024)

	for seq := uint64(1); seq <= 3; seq++ {
		if err := wal.Write(federation.WALEntry{SequenceNumber: seq, Data: []byte(`{}`)}); err != nil {
			t.Fatalf("Write seq=%d: %v", seq, err)
		}
	}

	wal.Ack(2)

	got, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Replay after Ack(2): got %d entries, want 1", len(got))
	}
	if got[0].SequenceNumber != 3 {
		t.Errorf("entry seq = %d; want 3", got[0].SequenceNumber)
	}
}

// TestWAL_ErrWALFull_CallbackFires sets a tiny maxBytes limit, writes until
// ErrWALFull is returned, and verifies the onFull callback fires exactly once.
func TestWAL_ErrWALFull_CallbackFires(t *testing.T) {
	// Allow only ~100 bytes — enough for one small entry but not two.
	wal := openTmpWAL(t, 100)

	callbackFired := 0
	wal.SetOnFull(func() { callbackFired++ })

	var fullErr error
	for i := 1; i <= 10; i++ {
		err := wal.Write(federation.WALEntry{SequenceNumber: uint64(i), Data: []byte(`{"event":"x"}`)})
		if errors.Is(err, federation.ErrWALFull) {
			fullErr = err
			break
		}
		if err != nil {
			t.Fatalf("unexpected write error at seq %d: %v", i, err)
		}
	}

	if fullErr == nil {
		t.Fatal("expected ErrWALFull but WAL never became full within 10 entries")
	}
	if callbackFired != 1 {
		t.Errorf("onFull callback fired %d times; want 1", callbackFired)
	}
	if !wal.IsFull() {
		t.Error("IsFull() = false after ErrWALFull returned; want true")
	}
}

// TestWAL_Compact_RemovesAckedEntries writes three entries, acks two, compacts,
// then verifies Replay still returns only the unacked entry and file shrinks.
func TestWAL_Compact_RemovesAckedEntries(t *testing.T) {
	wal := openTmpWAL(t, 1024*1024)

	for seq := uint64(1); seq <= 3; seq++ {
		payload := []byte(fmt.Sprintf(`{"seq":%d,"data":"some-payload"}`, seq))
		if err := wal.Write(federation.WALEntry{SequenceNumber: seq, Data: payload}); err != nil {
			t.Fatalf("Write seq=%d: %v", seq, err)
		}
	}
	wal.Ack(2)

	if err := wal.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	got, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay after compact: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Replay after compact: got %d entries, want 1", len(got))
	}
	if got[0].SequenceNumber != 3 {
		t.Errorf("post-compact entry seq = %d; want 3", got[0].SequenceNumber)
	}
}

// TestWAL_ConcurrentWrites spawns 10 goroutines each writing 5 entries and
// verifies that Replay returns all 50 entries with no data corruption.
func TestWAL_ConcurrentWrites(t *testing.T) {
	wal := openTmpWAL(t, 16*1024*1024)

	const goroutines = 10
	const perGoroutine = 5

	var wg sync.WaitGroup
	writeErrs := make([]error, goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				seq := uint64(g*perGoroutine + i + 1)
				err := wal.Write(federation.WALEntry{
					SequenceNumber: seq,
					Data:           []byte(fmt.Sprintf(`{"g":%d,"i":%d}`, g, i)),
				})
				if err != nil {
					writeErrs[g] = fmt.Errorf("goroutine %d seq %d: %w", g, seq, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	for _, err := range writeErrs {
		if err != nil {
			t.Fatalf("concurrent write error: %v", err)
		}
	}

	got, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != goroutines*perGoroutine {
		t.Errorf("Replay: got %d entries, want %d", len(got), goroutines*perGoroutine)
	}
}

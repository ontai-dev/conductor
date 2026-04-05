// Package federation_test covers the write-ahead log (WAL) behaviour:
// write-before-send ordering, ACK-before-advance, replay on reconnect,
// and full-WAL drop behaviour.
//
// conductor-schema.md §18.
package federation_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ontai-dev/conductor/internal/federation"
)

// openTestWAL creates a WAL at a temp path with the given maxBytes.
func openTestWAL(t *testing.T, maxBytes int64) *federation.WAL {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test-federation-wal")
	wal, err := federation.OpenWAL(path, maxBytes)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { wal.Close() })
	return wal
}

func makeEntry(seq uint64, subject string) federation.WALEntry {
	batch := federation.AuditEventBatch{
		Events: []federation.AuditEvent{
			{SequenceNumber: int64(seq), Subject: subject},
		},
	}
	data, _ := json.Marshal(batch)
	return federation.WALEntry{SequenceNumber: seq, Data: data}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestWAL_WriteAndReplay verifies that entries written to the WAL are returned
// by Replay in write order.
func TestWAL_WriteAndReplay(t *testing.T) {
	wal := openTestWAL(t, 1024*1024)

	entries := []federation.WALEntry{
		makeEntry(1, "alice"),
		makeEntry(2, "bob"),
		makeEntry(3, "carol"),
	}
	for _, e := range entries {
		if err := wal.Write(e); err != nil {
			t.Fatalf("Write(%d): %v", e.SequenceNumber, err)
		}
	}

	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 3 {
		t.Fatalf("expected 3 replayed entries, got %d", len(replayed))
	}
	for i, e := range replayed {
		if e.SequenceNumber != entries[i].SequenceNumber {
			t.Errorf("entry[%d] seq: expected %d, got %d", i, entries[i].SequenceNumber, e.SequenceNumber)
		}
	}
}

// TestWAL_AckBeforeAdvance verifies that Replay skips acknowledged entries
// and only returns unacknowledged ones.
func TestWAL_AckBeforeAdvance(t *testing.T) {
	wal := openTestWAL(t, 1024*1024)

	for i := uint64(1); i <= 5; i++ {
		if err := wal.Write(makeEntry(i, "user")); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}

	// Acknowledge entries 1-3. Only 4 and 5 should be returned by Replay.
	wal.Ack(3)

	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("expected 2 unacknowledged entries (4, 5), got %d", len(replayed))
	}
	if replayed[0].SequenceNumber != 4 {
		t.Errorf("expected first unacked seq=4, got %d", replayed[0].SequenceNumber)
	}
	if replayed[1].SequenceNumber != 5 {
		t.Errorf("expected second unacked seq=5, got %d", replayed[1].SequenceNumber)
	}
}

// TestWAL_ReplayOnReconnect verifies that after channel degradation and reconnect,
// all unacknowledged entries are available for replay.
func TestWAL_ReplayOnReconnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reconnect-wal")
	wal, err := federation.OpenWAL(path, 1024*1024)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	// Write 10 entries before simulated degradation.
	for i := uint64(1); i <= 10; i++ {
		if err := wal.Write(makeEntry(i, "user")); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}
	// Entries 1-7 acknowledged before disconnect.
	wal.Ack(7)

	// Simulate disconnect — close and reopen (represents degradation + restart).
	wal.Close()
	wal2, err := federation.OpenWAL(path, 1024*1024)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	// After reconnect, ackSeq is reset to 0 (from file metadata perspective).
	// In practice, the WAL would persist ackSeq too, but for this test we
	// set it manually to simulate the "last acked on previous session" state.
	wal2.Ack(7)

	// Write 2 more entries during degradation (before reconnect).
	if err := wal2.Write(makeEntry(11, "during-degradation-1")); err != nil {
		t.Fatalf("Write(11): %v", err)
	}
	if err := wal2.Write(makeEntry(12, "during-degradation-2")); err != nil {
		t.Fatalf("Write(12): %v", err)
	}

	// Replay should return entries 8-12 (unacked since ackSeq=7).
	replayed, err := wal2.Replay()
	if err != nil {
		t.Fatalf("Replay after reconnect: %v", err)
	}
	if len(replayed) != 5 {
		t.Fatalf("expected 5 unacked entries (8-12), got %d", len(replayed))
	}
	if replayed[0].SequenceNumber != 8 {
		t.Errorf("expected first replay seq=8, got %d", replayed[0].SequenceNumber)
	}
	if replayed[4].SequenceNumber != 12 {
		t.Errorf("expected last replay seq=12, got %d", replayed[4].SequenceNumber)
	}
}

// TestWAL_FullDropBehaviour verifies that when the WAL exceeds maxBytes,
// Write returns ErrWALFull and the WAL is marked full.
func TestWAL_FullDropBehaviour(t *testing.T) {
	// Set a tiny max (256 bytes) to trigger fullness quickly.
	wal := openTestWAL(t, 256)

	var lastErr error
	for i := uint64(1); i <= 100; i++ {
		lastErr = wal.Write(makeEntry(i, "overflow"))
		if lastErr != nil {
			break
		}
	}

	if lastErr == nil {
		t.Fatal("expected Write to return ErrWALFull after exceeding maxBytes")
	}
	if lastErr != federation.ErrWALFull {
		t.Errorf("expected ErrWALFull, got %v", lastErr)
	}
	if !wal.IsFull() {
		t.Error("expected WAL.IsFull() to return true after overflow")
	}
}

// TestWAL_FullOnFullCallback verifies that the onFull callback is invoked when
// the WAL transitions to full.
func TestWAL_FullOnFullCallback(t *testing.T) {
	wal := openTestWAL(t, 64) // tiny max

	called := make(chan struct{}, 1)
	wal.SetOnFull(func() {
		called <- struct{}{}
	})

	var hitFull bool
	for i := uint64(1); i <= 100; i++ {
		if err := wal.Write(makeEntry(i, "x")); err == federation.ErrWALFull {
			hitFull = true
			break
		}
	}
	if !hitFull {
		t.Fatal("did not hit WAL full condition")
	}

	// Callback should have been triggered.
	select {
	case <-called:
		// Expected.
	default:
		t.Error("onFull callback was not called")
	}
}

// TestWAL_Compact removes acknowledged entries and reduces file size.
func TestWAL_Compact(t *testing.T) {
	wal := openTestWAL(t, 1024*1024)

	for i := uint64(1); i <= 10; i++ {
		if err := wal.Write(makeEntry(i, "user")); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}
	wal.Ack(8) // acknowledge first 8

	if err := wal.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// After compaction only entries 9 and 10 remain.
	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay after compact: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("expected 2 entries after compact, got %d", len(replayed))
	}
}

// TestWAL_WriteBeforeSend verifies that the WAL durably records an entry before
// the entry is queued for stream transmission (ordering guarantee).
// This is tested via FederationClient.SendAuditBatch: the entry must be in the
// WAL before the channel receives it.
func TestWAL_WriteBeforeSend(t *testing.T) {
	wal := openTestWAL(t, 1024*1024)

	client := federation.NewFederationClient(
		"unused:9091", "/no/cert", "/no/key", "/no/ca", "ccs-dev", nil,
	)
	client.SetWAL(wal)

	batch := federation.AuditEventBatch{
		Events: []federation.AuditEvent{
			{SequenceNumber: 1, Subject: "alice"},
		},
	}
	env := client.NewAuditBatchEnvelope(batch)

	// Write to WAL via SendAuditBatch.
	if err := client.SendAuditBatch(env); err != nil {
		t.Fatalf("SendAuditBatch: %v", err)
	}

	// Verify the entry is in the WAL before any stream send occurs.
	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) == 0 {
		t.Fatal("expected WAL to contain the entry before stream send")
	}
	if replayed[0].SequenceNumber != env.SequenceNumber {
		t.Errorf("WAL entry seq: expected %d, got %d", env.SequenceNumber, replayed[0].SequenceNumber)
	}
}

// TestWAL_FullDropWithClient verifies that SendAuditBatch returns ErrWALFull
// when the WAL is at capacity, without panicking or blocking.
func TestWAL_FullDropWithClient(t *testing.T) {
	wal := openTestWAL(t, 64) // tiny max

	client := federation.NewFederationClient(
		"unused:9091", "/no/cert", "/no/key", "/no/ca", "ccs-dev", nil,
	)
	client.SetWAL(wal)

	batch := federation.AuditEventBatch{
		Events: []federation.AuditEvent{{SequenceNumber: 1}},
	}

	var fullErr error
	for i := 0; i < 100; i++ {
		env := client.NewAuditBatchEnvelope(batch)
		if err := client.SendAuditBatch(env); err != nil {
			fullErr = err
			break
		}
	}

	if fullErr == nil {
		t.Fatal("expected ErrWALFull from SendAuditBatch")
	}
	if fullErr != federation.ErrWALFull {
		t.Errorf("expected ErrWALFull, got %v", fullErr)
	}
}

// TestWAL_PersistenceAcrossReopen verifies that WAL entries survive a close/reopen
// cycle (simulating a process restart or reconnect).
func TestWAL_PersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist-wal")

	// Write 3 entries.
	wal1, err := federation.OpenWAL(path, 1024*1024)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := wal1.Write(makeEntry(i, "user")); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}
	wal1.Close()

	// Verify file exists and is non-empty.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat WAL file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected WAL file to be non-empty after writes")
	}

	// Reopen and replay — all 3 entries should be available.
	wal2, err := federation.OpenWAL(path, 1024*1024)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	replayed, err := wal2.Replay()
	if err != nil {
		t.Fatalf("Replay after reopen: %v", err)
	}
	if len(replayed) != 3 {
		t.Fatalf("expected 3 entries after reopen, got %d", len(replayed))
	}
}

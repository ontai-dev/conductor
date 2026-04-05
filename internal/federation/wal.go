package federation

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// DefaultWALPath is the default file path for the federation write-ahead log.
// Backed by a PVC in ont-system in production. conductor-schema.md §18.
const DefaultWALPath = "/var/lib/conductor/federation-wal"

// DefaultWALMaxBytes is the default maximum WAL file size (100MB).
// When the WAL is full, new events are dropped and AuditWALFull is set.
// conductor-schema.md §18.
const DefaultWALMaxBytes = 100 * 1024 * 1024 // 100 MB

// AuditWALFull is the condition name set on the tenant Conductor RunnerConfig
// status when the WAL has reached its maximum size. conductor-schema.md §18.
const AuditWALFull = "AuditWALFull"

// WALEntry is a single entry in the write-ahead log.
// Each entry carries a sequence number and a JSON-encoded Envelope.
type WALEntry struct {
	// SequenceNumber is the outgoing federation envelope sequence number.
	// Matches Envelope.SequenceNumber for ACK correlation.
	SequenceNumber uint64 `json:"seq"`

	// Data is the JSON-encoded Envelope payload.
	Data []byte `json:"data"`
}

// WAL is the federation write-ahead log. It is append-only and backed by a
// single file on a PVC in ont-system. The WAL persists unacknowledged audit
// event batches across reconnection cycles. conductor-schema.md §18.
//
// Wire format per entry:
//   [4-byte big-endian uint32 length][JSON-encoded WALEntry]
//
// The WAL maintains two pointers:
//   - writeOffset: byte position of the next write (end of file).
//   - readOffset:  byte position of the next entry to send.
//
// Acknowledged entries are not immediately truncated — they remain on disk
// until the WAL is compacted (at startup or when free space falls below a threshold).
// For simplicity in this implementation, compaction happens at WAL open time: entries
// before ackSeq are skipped on replay.
type WAL struct {
	mu         sync.Mutex
	path       string
	maxBytes   int64
	file       *os.File
	writeBytes int64 // total bytes written (approximation for max check)
	ackSeq     uint64 // highest acknowledged sequence number
	isFull     bool

	// onFull is called when the WAL transitions to full. May be nil.
	onFull func()
}

// OpenWAL opens (or creates) the WAL file at path.
// If the file exists, its current size is read to initialise writeBytes.
// ackSeq defaults to 0 (all entries are unacknowledged until Ack is called).
func OpenWAL(path string, maxBytes int64) (*WAL, error) {
	if path == "" {
		path = DefaultWALPath
	}
	if maxBytes <= 0 {
		maxBytes = DefaultWALMaxBytes
	}

	// Ensure parent directory exists.
	dir := walDir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create WAL directory %q: %w", dir, err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL %q: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat WAL %q: %w", path, err)
	}

	return &WAL{
		path:       path,
		maxBytes:   maxBytes,
		file:       f,
		writeBytes: info.Size(),
	}, nil
}

// walDir returns the directory component of path, defaulting to "." for bare filenames.
func walDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[:i]
		}
	}
	return "."
}

// Close closes the underlying WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

// IsFull returns true when the WAL has reached its maximum byte size.
func (w *WAL) IsFull() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isFull
}

// SetOnFull sets a callback that is invoked when the WAL transitions to full.
func (w *WAL) SetOnFull(f func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.onFull = f
}

// Write appends a WALEntry to the WAL.
//
// Returns ErrWALFull when the WAL has reached maxBytes. The caller (AuditForwarder)
// must handle this by dropping the event and emitting an AuditWALFull condition.
// conductor-schema.md §18.
func (w *WAL) Write(entry WALEntry) error {
	w.mu.Lock()

	if w.isFull {
		w.mu.Unlock()
		return ErrWALFull
	}
	if w.file == nil {
		w.mu.Unlock()
		return fmt.Errorf("WAL is closed")
	}

	payload, err := json.Marshal(entry)
	if err != nil {
		w.mu.Unlock()
		return fmt.Errorf("marshal WAL entry: %w", err)
	}

	// Encode length prefix (4-byte big-endian uint32).
	if len(payload) > 1<<30 {
		w.mu.Unlock()
		return fmt.Errorf("WAL entry too large: %d bytes", len(payload))
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	// Check size before writing. Capture onFull callback under lock, invoke after.
	entrySize := int64(4 + len(payload))
	if w.writeBytes+entrySize > w.maxBytes {
		w.isFull = true
		onFull := w.onFull
		w.mu.Unlock()
		if onFull != nil {
			onFull()
		}
		return ErrWALFull
	}

	if _, err := w.file.Write(header); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("write WAL header: %w", err)
	}
	if _, err := w.file.Write(payload); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("write WAL payload: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("sync WAL: %w", err)
	}

	w.writeBytes += entrySize
	w.mu.Unlock()
	return nil
}

// Ack records that all entries up to and including seq have been acknowledged
// by the management side. The read pointer is advanced past acknowledged entries
// on the next Replay call. conductor-schema.md §18.
func (w *WAL) Ack(seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq > w.ackSeq {
		w.ackSeq = seq
	}
}

// AckSeq returns the current acknowledged sequence number.
func (w *WAL) AckSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ackSeq
}

// Replay reads all WALEntry values from the file whose SequenceNumber is greater
// than the current ackSeq (i.e. unacknowledged entries). Returns entries in
// write order. conductor-schema.md §18.
func (w *WAL) Replay() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil, fmt.Errorf("WAL is closed")
	}

	// Seek to beginning to read from start.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek WAL: %w", err)
	}

	var entries []WALEntry
	header := make([]byte, 4)
	ackSeq := w.ackSeq

	for {
		// Read length prefix.
		if _, err := io.ReadFull(w.file, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read WAL header: %w", err)
		}
		length := binary.BigEndian.Uint32(header)

		payload := make([]byte, length)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, fmt.Errorf("read WAL payload: %w", err)
		}

		var entry WALEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			// Skip malformed entries.
			continue
		}

		// Only return unacknowledged entries.
		if entry.SequenceNumber > ackSeq {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// Compact truncates the WAL file, removing all acknowledged entries.
// After compaction, Replay will only return entries with SequenceNumber > ackSeq.
// Compaction is run at startup and periodically to keep the WAL file bounded.
// conductor-schema.md §18.
func (w *WAL) Compact() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return fmt.Errorf("WAL is closed")
	}

	// Read all unacknowledged entries.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL for compaction: %w", err)
	}

	var keep []WALEntry
	header := make([]byte, 4)
	ackSeq := w.ackSeq

	for {
		if _, err := io.ReadFull(w.file, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("compact read header: %w", err)
		}
		length := binary.BigEndian.Uint32(header)
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("compact read payload: %w", err)
		}
		var entry WALEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			continue
		}
		if entry.SequenceNumber > ackSeq {
			keep = append(keep, entry)
		}
	}

	// Truncate and rewrite.
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek after truncate: %w", err)
	}

	var newSize int64
	for _, entry := range keep {
		payload, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal entry during compact: %w", err)
		}
		hdr := make([]byte, 4)
		binary.BigEndian.PutUint32(hdr, uint32(len(payload)))
		if _, err := w.file.Write(hdr); err != nil {
			return fmt.Errorf("write header during compact: %w", err)
		}
		if _, err := w.file.Write(payload); err != nil {
			return fmt.Errorf("write payload during compact: %w", err)
		}
		newSize += int64(4 + len(payload))
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync after compact: %w", err)
	}

	w.writeBytes = newSize
	// Clear full flag after compaction frees space.
	if w.isFull && newSize < w.maxBytes {
		w.isFull = false
	}

	return nil
}

// ErrWALFull is returned by Write when the WAL has reached its maximum size.
var ErrWALFull = fmt.Errorf("WAL is full: maximum size reached, dropping audit event")

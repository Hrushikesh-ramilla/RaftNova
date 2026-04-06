package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	RecordTerm  uint32 = 1 // {"term": 42}
	RecordVote  uint32 = 2 // {"voted_for": "node-2", "term": 42}
	RecordEntry uint32 = 3 // {"index": 10, "term": 42, "command": "base64..."}

	maxWALSize     = 64 * 1024 * 1024 // 64MB rotation threshold
	walFilePattern = "wal_%06d.log"
)

// TermRecord is the JSON payload for a RecordTerm WAL entry.
type TermRecord struct {
	Term uint64 `json:"term"`
}

// VoteRecord is the JSON payload for a RecordVote WAL entry.
type VoteRecord struct {
	VotedFor string `json:"voted_for"`
	Term     uint64 `json:"term"`
}

// EntryRecord is the JSON payload for a RecordEntry WAL entry.
type EntryRecord struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command"`
}

// WAL implements an append-only write-ahead log with CRC32 checksums and
// fsync on every write. Record format:
//
//	[4 bytes: record type] [4 bytes: payload length] [N bytes: payload JSON] [4 bytes: CRC32]
//
// Corruption handling: on replay, truncate at the last valid CRC boundary.
// WAL rotation: when a file exceeds 64MB, start a new file.
type WAL struct {
	dir      string
	file     *os.File
	fileID   int
	fileSize int64
	logger   *slog.Logger
}

// NewWAL creates or opens a WAL in the given directory.
func NewWAL(dir string, logger *slog.Logger) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}

	w := &WAL{
		dir:    dir,
		logger: logger,
	}

	files, err := w.listWALFiles()
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		w.fileID = 1
	} else {
		w.fileID = files[len(files)-1].id
	}

	if err := w.openCurrentFile(); err != nil {
		return nil, err
	}

	return w, nil
}

type walFile struct {
	id   int
	name string
	path string
}

func (w *WAL) listWALFiles() ([]walFile, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, fmt.Errorf("list WAL dir: %w", err)
	}

	var files []walFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "wal_") {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(e.Name(), walFilePattern, &id); err != nil {
			continue
		}
		files = append(files, walFile{
			id:   id,
			name: e.Name(),
			path: filepath.Join(w.dir, e.Name()),
		})
	}

	sort.Slice(files, func(i, j int) bool { return files[i].id < files[j].id })
	return files, nil
}

func (w *WAL) openCurrentFile() error {
	path := filepath.Join(w.dir, fmt.Sprintf(walFilePattern, w.fileID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open WAL file: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("stat WAL file: %w", err)
	}

	w.file = f
	w.fileSize = info.Size()
	return nil
}

// Append writes a record to the WAL with CRC32 checksum and calls fsync.
func (w *WAL) Append(recordType uint32, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal WAL payload: %w", err)
	}

	// Check if we need to rotate before writing.
	recordSize := int64(4 + 4 + len(data) + 4)
	if w.fileSize+recordSize > maxWALSize {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("WAL rotation: %w", err)
		}
	}

	// Write: [type:4][length:4][payload:N][crc32:4]
	buf := make([]byte, 4+4+len(data)+4)
	binary.LittleEndian.PutUint32(buf[0:4], recordType)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(data)))
	copy(buf[8:8+len(data)], data)

	checksum := crc32.ChecksumIEEE(buf[:8+len(data)])
	binary.LittleEndian.PutUint32(buf[8+len(data):], checksum)

	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("write WAL record: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("fsync WAL: %w", err)
	}

	w.fileSize += int64(len(buf))
	return nil
}

func (w *WAL) rotate() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("close WAL file for rotation: %w", err)
		}
	}

	w.fileID++
	w.logger.Info("WAL rotation", "new_file_id", w.fileID)
	return w.openCurrentFile()
}

// Replay reads all WAL files from the beginning and reconstructs state.
// Returns the latest term, votedFor, and all log entries.
// Stops at the first corrupt record and truncates the remainder.
func (w *WAL) Replay() (term uint64, votedFor string, entries []EntryRecord, err error) {
	files, err := w.listWALFiles()
	if err != nil {
		return 0, "", nil, err
	}

	for _, wf := range files {
		t, v, e, replayErr := w.replayFile(wf.path)
		if replayErr != nil {
			w.logger.Warn("WAL replay stopped at corrupt record",
				"file", wf.name,
				"error", replayErr,
			)
			// Truncate at corruption point — only for the last file.
			break
		}

		// Apply records: last term/vote wins, entries accumulate.
		if t > 0 {
			term = t
		}
		if v != "" {
			votedFor = v
		}
		entries = append(entries, e...)
	}

	return term, votedFor, entries, nil
}

func (w *WAL) replayFile(path string) (lastTerm uint64, lastVotedFor string, entries []EntryRecord, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", nil, fmt.Errorf("open WAL for replay: %w", err)
	}
	defer f.Close()

	var validOffset int64

	for {
		// Read header: [type:4][length:4]
		var header [8]byte
		if _, err := io.ReadFull(f, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // Clean end of file or partial header
			}
			return lastTerm, lastVotedFor, entries, fmt.Errorf("read WAL header: %w", err)
		}

		recordType := binary.LittleEndian.Uint32(header[0:4])
		payloadLen := binary.LittleEndian.Uint32(header[4:8])

		// Read payload + CRC
		payloadAndCRC := make([]byte, payloadLen+4)
		if _, err := io.ReadFull(f, payloadAndCRC); err != nil {
			// Truncate at this record — it's incomplete.
			w.logger.Warn("WAL truncating at incomplete record", "offset", validOffset)
			if truncErr := w.truncateFile(path, validOffset); truncErr != nil {
				w.logger.Error("WAL truncation failed", "error", truncErr)
			}
			break
		}

		payload := payloadAndCRC[:payloadLen]
		storedCRC := binary.LittleEndian.Uint32(payloadAndCRC[payloadLen:])

		// Verify CRC over header + payload.
		var crcBuf []byte
		crcBuf = append(crcBuf, header[:]...)
		crcBuf = append(crcBuf, payload...)
		computedCRC := crc32.ChecksumIEEE(crcBuf)

		if storedCRC != computedCRC {
			w.logger.Warn("WAL CRC mismatch — truncating at last valid record",
				"offset", validOffset,
				"stored_crc", storedCRC,
				"computed_crc", computedCRC,
			)
			if truncErr := w.truncateFile(path, validOffset); truncErr != nil {
				w.logger.Error("WAL truncation failed", "error", truncErr)
			}
			break
		}

		// Record is valid — process it.
		switch recordType {
		case RecordTerm:
			var rec TermRecord
			if err := json.Unmarshal(payload, &rec); err != nil {
				w.logger.Warn("WAL bad term record", "error", err)
				break
			}
			lastTerm = rec.Term

		case RecordVote:
			var rec VoteRecord
			if err := json.Unmarshal(payload, &rec); err != nil {
				w.logger.Warn("WAL bad vote record", "error", err)
				break
			}
			lastVotedFor = rec.VotedFor
			if rec.Term > lastTerm {
				lastTerm = rec.Term
			}

		case RecordEntry:
			var rec EntryRecord
			if err := json.Unmarshal(payload, &rec); err != nil {
				w.logger.Warn("WAL bad entry record", "error", err)
				break
			}
			entries = append(entries, rec)
		}

		validOffset += int64(8 + payloadLen + 4)
	}

	return lastTerm, lastVotedFor, entries, nil
}

func (w *WAL) truncateFile(path string, offset int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(offset)
}

// PersistTerm writes a term record to the WAL.
func (w *WAL) PersistTerm(term uint64) error {
	return w.Append(RecordTerm, TermRecord{Term: term})
}

// PersistVote writes a vote record to the WAL.
func (w *WAL) PersistVote(votedFor string, term uint64) error {
	return w.Append(RecordVote, VoteRecord{VotedFor: votedFor, Term: term})
}

// PersistEntry writes a log entry record to the WAL.
func (w *WAL) PersistEntry(index, term uint64, command []byte) error {
	return w.Append(RecordEntry, EntryRecord{Index: index, Term: term, Command: command})
}

// Close closes the current WAL file.
func (w *WAL) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

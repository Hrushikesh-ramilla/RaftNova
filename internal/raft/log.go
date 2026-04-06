package raft

// RaftLog maintains the Raft log entries. It is 1-indexed — index 0 is a
// sentinel entry that simplifies boundary checks. The log is not independently
// thread-safe; it is protected by the Node's mutex.
type RaftLog struct {
	entries []LogEntry
}

// NewRaftLog creates a new log with a sentinel entry at index 0.
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries: []LogEntry{{Term: 0, Index: 0}}, // sentinel
	}
}

// Append adds entries to the end of the log.
func (l *RaftLog) Append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// Entry returns the log entry at the given index.
// Caller must ensure index is within bounds.
func (l *RaftLog) Entry(idx LogIndex) LogEntry {
	return l.entries[idx]
}

// LastIndex returns the index of the last log entry.
func (l *RaftLog) LastIndex() LogIndex {
	return LogIndex(len(l.entries) - 1)
}

// LastTerm returns the term of the last log entry.
func (l *RaftLog) LastTerm() Term {
	return l.entries[len(l.entries)-1].Term
}

// TruncateAfter removes all entries after the given index.
// Entries at or before the index are kept.
func (l *RaftLog) TruncateAfter(idx LogIndex) {
	if int(idx) < len(l.entries)-1 {
		l.entries = l.entries[:idx+1]
	}
}

// Entries returns a copy of entries from startIdx to endIdx inclusive.
func (l *RaftLog) Entries(startIdx, endIdx LogIndex) []LogEntry {
	if startIdx > endIdx || int(startIdx) >= len(l.entries) {
		return nil
	}
	if int(endIdx) >= len(l.entries) {
		endIdx = LogIndex(len(l.entries) - 1)
	}
	result := make([]LogEntry, endIdx-startIdx+1)
	copy(result, l.entries[startIdx:endIdx+1])
	return result
}

// EntriesFrom returns a copy of all entries starting from startIdx.
func (l *RaftLog) EntriesFrom(startIdx LogIndex) []LogEntry {
	return l.Entries(startIdx, l.LastIndex())
}

// Len returns the number of entries including the sentinel.
func (l *RaftLog) Len() int {
	return len(l.entries)
}

// Contains returns true if the log has an entry at the given index.
func (l *RaftLog) Contains(idx LogIndex) bool {
	return int(idx) < len(l.entries)
}

// TermAt returns the term of the entry at the given index, or 0 if out of range.
func (l *RaftLog) TermAt(idx LogIndex) Term {
	if !l.Contains(idx) {
		return 0
	}
	return l.entries[idx].Term
}

// RestoreEntries replaces all entries (keeping sentinel) with the given entries.
// Used during WAL replay on startup.
func (l *RaftLog) RestoreEntries(entries []LogEntry) {
	l.entries = []LogEntry{{Term: 0, Index: 0}}
	l.entries = append(l.entries, entries...)
}

// FindFirstIndexOfTerm returns the first index in the log with the given term.
// Returns 0 if not found.
func (l *RaftLog) FindFirstIndexOfTerm(term Term) LogIndex {
	for i := 1; i < len(l.entries); i++ {
		if l.entries[i].Term == term {
			return LogIndex(i)
		}
	}
	return 0
}

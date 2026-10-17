package sync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/erichll/go-fast-note-sync/internal/config"
	h "github.com/erichll/go-fast-note-sync/internal/hash"
)

// A dropped round must not advance lastTime: the next round would skip
// server pages that were never processed.
func TestSyncTimeHeldUntilRoundCompletes(t *testing.T) {
	svc := newTestService(nil, nil, filepath.Join(t.TempDir(), "state.json"))
	svc.st.NoteSyncTime = 100
	svc.mu.Lock()
	svc.isSyncing = true
	svc.syncRoundID = 7
	svc.roundSyncTimes = map[string]int64{}
	svc.mu.Unlock()

	svc.updateSyncTime("note", 500)
	if svc.st.NoteSyncTime != 100 {
		t.Fatalf("NoteSyncTime advanced mid-round: %d", svc.st.NoteSyncTime)
	}
	// Disconnect abandons the round: held time is discarded.
	svc.abandonActiveSync("disconnect")
	if svc.st.NoteSyncTime != 100 {
		t.Fatalf("NoteSyncTime advanced after abandon: %d", svc.st.NoteSyncTime)
	}

	svc.mu.Lock()
	svc.isSyncing = true
	svc.syncRoundID = 9
	svc.roundSyncTimes = map[string]int64{}
	svc.mu.Unlock()
	svc.updateSyncTime("note", 600)
	svc.onSyncComplete(9, true)
	if svc.st.NoteSyncTime != 600 {
		t.Fatalf("NoteSyncTime = %d, want 600 after completion", svc.st.NoteSyncTime)
	}
	// Outside a round, live acks apply immediately.
	svc.updateSyncTime("note", 700)
	if svc.st.NoteSyncTime != 700 {
		t.Fatalf("NoteSyncTime = %d, want 700", svc.st.NoteSyncTime)
	}
}

// Notes the server did not act on are in sync and must leave pending, so the
// next round does not re-offer the whole vault.
func TestCompletedRoundCommitsUnaddressedNotes(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, body string) string {
		p := filepath.Join(dir, rel)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return h.Text(body)
	}
	hA := write("a.md", "alpha")
	hB := write("b.md", "beta")
	hC := write("c.md", "gamma")
	cfg := &config.Config{API: "http://x", VaultPath: dir}
	svc := newTestService(cfg, nil, filepath.Join(t.TempDir(), "state.json"))
	for p, hh := range map[string]string{"a.md": hA, "b.md": hB, "c.md": hC} {
		svc.setPendingNoteModify(p, hh)
	}
	write("c.md", "gamma edited locally") // changed after offer: must stay pending
	svc.mu.Lock()
	svc.isSyncing = true
	svc.syncRoundID = 3
	svc.roundNoteSent = map[string]string{"a.md": hA, "b.md": hB, "c.md": hC}
	svc.roundNoteAddressed = map[string]struct{}{}
	svc.mu.Unlock()
	svc.markNoteAddressed("b.md") // server asked for b: wait for its ack

	svc.onSyncComplete(3, true)

	if _, ok := svc.pendingNoteModifies["a.md"]; ok {
		t.Error("a.md should be committed")
	}
	if e := svc.st.FileHashMap["a.md"]; e.Hash != hA {
		t.Errorf("a.md hash = %q, want %q", e.Hash, hA)
	}
	if _, ok := svc.pendingNoteModifies["b.md"]; !ok {
		t.Error("b.md was addressed by server and must stay pending")
	}
	if _, ok := svc.pendingNoteModifies["c.md"]; !ok {
		t.Error("c.md changed locally and must stay pending")
	}
}

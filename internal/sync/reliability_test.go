package sync

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

// TestDisconnectMidScanDoesNotStickSyncing reproduces the permanent
// "already syncing, skipping" state: a vault scan that outlives the socket
// must not publish isSyncing/isSyncRequesting after the round is abandoned.
func TestDisconnectMidScanDoesNotStickSyncing(t *testing.T) {
	vault := t.TempDir()
	if err := os.WriteFile(filepath.Join(vault, "note.md"), []byte("# hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := newTestService(&config.Config{
		API:               "http://test.example.com",
		Vault:             "vault",
		VaultPath:         vault,
		ConfigSyncEnabled: false,
	}, state.New(), filepath.Join(t.TempDir(), "state.json"))
	svc.conn = newFakeWSConn()
	svc.isOpen = true
	svc.isAuth = true
	svc.syncTimeout = time.Hour
	svc.stopCh = make(chan struct{})

	reached := make(chan uint64, 1)
	svc.beforeCommitScan = func(roundID uint64) {
		reached <- roundID
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if !svc.ownsSyncRound(roundID) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Errorf("scan round %d was not abandoned", roundID)
	}

	svc.handleSync(false)
	var roundID uint64
	select {
	case roundID = <-reached:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not reach the commit point")
	}
	svc.abandonActiveSync("disconnect")

	deadline := time.Now().Add(2 * time.Second)
	for {
		svc.mu.Lock()
		syncing := svc.isSyncing
		requesting := svc.isSyncRequesting
		id := svc.syncRoundID
		svc.mu.Unlock()
		if !syncing && !requesting && id != roundID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stuck after disconnect: syncing=%v requesting=%v round=%d (scan round %d)", syncing, requesting, id, roundID)
		}
		time.Sleep(5 * time.Millisecond)
	}

	svc.beforeCommitScan = nil
	svc.syncTimeout = 30 * time.Millisecond
	svc.handleSync(true)
	svc.mu.Lock()
	started := svc.isSyncing
	svc.mu.Unlock()
	if !started {
		t.Fatal("later handleSync was skipped; syncing flag is stuck")
	}
	_ = svc.Close()
}

// TestReadCloseResetsSyncingFlag is the reconnect-path half of the stuck flag:
// when the socket drops mid-round, the next handleSync is not refused.
func TestReadCloseResetsSyncingFlag(t *testing.T) {
	svc := newTestService(nil, state.New(), filepath.Join(t.TempDir(), "state.json"))
	svc.httpDoer = &fakeHTTPDoer{statusCode: 200}
	svc.isRegister = false
	svc.isSyncing = true
	svc.isSyncRequesting = true
	svc.syncRoundID = 7
	svc.dialer = &fakeDialer{conn: &fakeWSConn{
		messages: []wsMsg{{err: gorillaws.ErrCloseSent}},
	}}

	svc.connectOnce()

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.isSyncing || svc.isSyncRequesting {
		t.Fatalf("disconnect left syncing=%v requesting=%v", svc.isSyncing, svc.isSyncRequesting)
	}
	if svc.syncRoundID == 7 {
		t.Fatal("disconnect did not retire the in-flight round")
	}
}

// TestFakeServerReconnectStartsANewRound drives the real dialer against an
// httptest websocket server. The first socket dies with a TCP close (the
// 1006 / unexpected EOF path) while a scan is in flight; the client must
// reconnect and finish a later round instead of logging "already syncing".
func TestFakeServerReconnectStartsANewRound(t *testing.T) {
	vault := t.TempDir()
	if err := os.WriteFile(filepath.Join(vault, "note.md"), []byte("body\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var dials atomic.Int32
	scanBlocked := make(chan struct{})
	var once stdsync.Once
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	const syncEnd = `{"code":1,"data":{"lastTime":1,"needUploadCount":0,"needModifyCount":0,"needSyncMtimeCount":0,"needDeleteCount":0}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/api/user/sync") {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		n := dials.Add(1)
		defer conn.Close()
		if n == 1 {
			go func() {
				select {
				case <-scanBlocked:
					time.Sleep(20 * time.Millisecond)
				case <-time.After(3 * time.Second):
				}
				_ = conn.NetConn().Close()
			}()
		}
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			text := string(data)
			var reply string
			switch {
			case strings.HasPrefix(text, "Authorization|"):
				reply = `Authorization|{"code":1}`
			case strings.HasPrefix(text, "ClientInfo|"):
				reply = `ClientInfo|{"code":1}`
			case strings.HasPrefix(text, "FolderSync|"):
				reply = "FolderSyncEnd|" + syncEnd
			case strings.HasPrefix(text, "NoteSync|"):
				reply = "NoteSyncEnd|" + syncEnd
			case strings.HasPrefix(text, "FileSync|"):
				reply = "FileSyncEnd|" + syncEnd
			}
			if reply != "" {
				_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if err := conn.WriteMessage(gorillaws.TextMessage, []byte(reply)); err != nil {
					return
				}
			}
		}
	}))
	t.Cleanup(srv.Close)

	cfg := &config.Config{
		API:               srv.URL,
		APIToken:          "token",
		Vault:             "vault",
		VaultPath:         vault,
		ConfigSyncEnabled: false,
	}
	svc := NewSyncService(cfg, state.New(), filepath.Join(t.TempDir(), "state.json"), "1.1.4")
	svc.sleepFn = func(time.Duration) {}
	svc.syncTimeout = 3 * time.Second
	svc.folderWaitPoll = 5 * time.Millisecond
	svc.folderWaitLimit = 2 * time.Second
	svc.beforeCommitScan = func(roundID uint64) {
		first := false
		once.Do(func() {
			first = true
			close(scanBlocked)
		})
		if !first {
			return
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if !svc.ownsSyncRound(roundID) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Errorf("first scan round %d survived the socket drop", roundID)
	}
	t.Cleanup(func() { _ = svc.Close() })

	svc.Connect()

	select {
	case <-svc.SyncComplete():
	case <-time.After(5 * time.Second):
		svc.mu.Lock()
		syncing, requesting, id := svc.isSyncing, svc.isSyncRequesting, svc.syncRoundID
		svc.mu.Unlock()
		t.Fatalf("no sync round completed after reconnect (dials=%d syncing=%v requesting=%v round=%d err=%v)",
			dials.Load(), syncing, requesting, id, svc.SyncError())
	}
	if dials.Load() < 2 {
		t.Fatalf("dials = %d, want a reconnect after the abnormal close", dials.Load())
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.isSyncing || svc.isSyncRequesting {
		t.Fatalf("completed reconnect left syncing=%v requesting=%v", svc.isSyncing, svc.isSyncRequesting)
	}
}

func TestPeriodicSyncContinuesAfterDisconnect(t *testing.T) {
	svc := newTestService(&config.Config{
		API:       "http://test.example.com",
		Vault:     "vault",
		VaultPath: t.TempDir(),
	}, state.New(), filepath.Join(t.TempDir(), "state.json"))
	svc.stopCh = make(chan struct{})
	svc.periodicInterval = 30 * time.Millisecond
	svc.isRegister = true
	svc.isOpen = true
	svc.isAuth = true
	svc.conn = newFakeWSConn()
	svc.syncTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = svc.Close() })

	go svc.periodicSyncLoop()

	waitRoundAbove := func(min uint64) uint64 {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			svc.mu.Lock()
			id := svc.syncRoundID
			svc.mu.Unlock()
			if id > min {
				return id
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("sync round did not advance past %d", min)
		return 0
	}

	first := waitRoundAbove(0)
	svc.mu.Lock()
	svc.isOpen = false
	svc.isAuth = false
	svc.abandonActiveSyncLocked("disconnect")
	svc.mu.Unlock()
	// A tick that already passed the ready check can still enter handleSync.
	time.Sleep(150 * time.Millisecond)
	svc.mu.Lock()
	svc.abandonActiveSyncLocked("disconnect")
	paused := svc.syncRoundID
	svc.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	svc.mu.Lock()
	during := svc.syncRoundID
	svc.isOpen = true
	svc.isAuth = true
	svc.conn = newFakeWSConn()
	svc.mu.Unlock()
	if during != paused {
		t.Fatalf("periodic loop started a round while disconnected: %d -> %d", paused, during)
	}
	if got := waitRoundAbove(paused); got <= first {
		t.Fatalf("round after reconnect = %d, first = %d", got, first)
	}
}

func TestPingHandlerPongUsesWriteWait(t *testing.T) {
	orig := writeWait
	writeWait = 2 * time.Second
	t.Cleanup(func() { writeWait = orig })

	conn := newFakeWSConn()
	if err := installKeepalive(conn); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	if err := conn.TriggerPing("server-ping"); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	var pong *controlFrame
	for i := range conn.controlWrites {
		if conn.controlWrites[i].messageType == gorillaws.PongMessage {
			pong = &conn.controlWrites[i]
			break
		}
	}
	if pong == nil {
		t.Fatal("server ping did not produce a pong")
	}
	if string(pong.data) != "server-ping" {
		t.Fatalf("pong payload = %q", pong.data)
	}
	if pong.deadline.Before(before.Add(writeWait-time.Second)) || pong.deadline.After(time.Now().Add(writeWait+time.Second)) {
		t.Fatalf("pong deadline %v is outside writeWait %v", pong.deadline, writeWait)
	}
	if conn.readDeadline.Before(before.Add(pongWait / 2)) {
		t.Fatal("server ping did not refresh the read deadline")
	}
}

func TestCloseAbandonsStuckSync(t *testing.T) {
	svc := newTestService(nil, state.New(), filepath.Join(t.TempDir(), "state.json"))
	svc.stopCh = make(chan struct{})
	svc.isSyncing = true
	svc.isSyncRequesting = true
	svc.syncRoundID = 3
	svc.conn = newFakeWSConn()

	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	if svc.isSyncing || svc.isSyncRequesting {
		t.Fatalf("Close left syncing=%v requesting=%v", svc.isSyncing, svc.isSyncRequesting)
	}
	if svc.syncRoundID == 3 {
		t.Fatal("Close did not retire the in-flight round")
	}
}

func TestWaitForSlotUnblocksOnStop(t *testing.T) {
	m := NewConcurrencyManager(&config.Config{ConcurrencyControlEnabled: true, MaxConcurrentUploads: 1})
	if !m.WaitForSlot("held", false, 0) {
		t.Fatal("first slot acquire failed")
	}
	done := make(chan bool, 1)
	go func() { done <- m.WaitForSlot("blocked", false, 0) }()
	time.Sleep(20 * time.Millisecond)
	m.Stop()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("WaitForSlot returned true after Stop")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForSlot blocked after Stop")
	}
}

func TestLockPathReturnsOnShutdown(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.stopCh = make(chan struct{})
	unlock := svc.lockPath("note.md")
	defer unlock()

	done := make(chan struct{})
	go func() {
		release := svc.lockPath("note.md")
		release()
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	close(svc.stopCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lockPath did not return after shutdown")
	}
}

func TestReconnectWaitCancelledByClose(t *testing.T) {
	svc := newTestService(nil, state.New(), filepath.Join(t.TempDir(), "state.json"))
	svc.stopCh = make(chan struct{})
	svc.isRegister = true
	svc.httpDoer = &fakeHTTPDoer{statusCode: 503}
	release := make(chan struct{})
	svc.sleepFn = func(time.Duration) { <-release }

	done := make(chan struct{})
	go func() {
		svc.scheduleReconnect()
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduleReconnect ignored shutdown")
	}
	close(release)
}

package sync

import (
	"io"
	"net/http"
	"path/filepath"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
)

func testManager(lease, poll, acquire time.Duration) *ConcurrencyManager {
	m := NewConcurrencyManager(&config.Config{ConcurrencyControlEnabled: true, MaxConcurrentUploads: 1})
	m.leaseTimeout, m.pollInterval, m.acquireTimeout = lease, poll, acquire
	return m
}

func (f *fakeWSConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// A slot whose Ack never arrives used to stay held until restart, so every
// later push timed out ("concurrency slot acquire timed out after 1m0s").
func TestConcurrencyReclaimsStaleLease(t *testing.T) {
	m := testManager(50*time.Millisecond, 5*time.Millisecond, 2*time.Second)
	if !m.WaitForSlot("a", false, 0) {
		t.Fatal("first acquire failed")
	}
	start := time.Now()
	if !m.WaitForSlot("b", false, 0) {
		t.Fatal("stale lease was not reclaimed")
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("reclaim took %v", waited)
	}
	if held, _ := m.HeldSlots(); held != 1 {
		t.Fatalf("held slots = %d, want 1", held)
	}
	// Rename lane too.
	if !m.WaitForSlot("r1", true, 0) || !m.WaitForSlot("r2", true, 0) {
		t.Fatal("stale rename lease was not reclaimed")
	}
}

func TestConcurrencyReleaseWakesWaiter(t *testing.T) {
	m := testManager(time.Hour, time.Hour, 2*time.Second) // only a release can wake it
	m.WaitForSlot("a", false, 0)
	got := make(chan bool, 1)
	go func() { got <- m.WaitForSlot("b", false, 0) }()
	time.Sleep(20 * time.Millisecond)
	m.ReleaseSlot("a")
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("waiter failed")
		}
	case <-time.After(time.Second):
		t.Fatal("release did not wake the waiter")
	}
}

func TestConcurrencyLiveLeaseStillLimits(t *testing.T) {
	m := testManager(time.Hour, 5*time.Millisecond, 50*time.Millisecond)
	m.WaitForSlot("a", false, 0)
	if m.WaitForSlot("b", false, 0) {
		t.Fatal("fresh lease should still limit concurrency")
	}
}

func TestAuthRetryDelay(t *testing.T) {
	cases := map[int]time.Duration{
		0: 30 * time.Second, 1: 30 * time.Second, 2: time.Minute, 3: 2 * time.Minute,
		5: 8 * time.Minute, 6: 10 * time.Minute, 1000: 10 * time.Minute,
	}
	for n, want := range cases {
		if got := authRetryDelay(n); got != want {
			t.Errorf("authRetryDelay(%d) = %v, want %v", n, got, want)
		}
	}
}

// Seen in production: one "Code=315 Auth token Scope restricted" rejection
// and the client never reconnected. A rejection now closes the socket and
// marks it for the slow retry path.
func TestAuthRejectionClosesSocketForSlowRetry(t *testing.T) {
	svc := newTestService(&config.Config{API: "http://x", APIToken: "tok"}, nil, "")
	conn := newFakeWSConn()
	svc.conn = conn
	svc.isOpen = true
	handleAuthorization(Envelope{Code: 315, Message: "Auth token Scope restricted"}, svc)
	if !conn.isClosed() {
		t.Fatal("rejected socket was left open")
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if !svc.authFailed || svc.isAuth {
		t.Fatalf("authFailed=%v isAuth=%v", svc.authFailed, svc.isAuth)
	}
}

func TestAuthSuccessResetsBackoff(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.authFailures = 4
	svc.reconnectAttempts = 9
	svc.conn = newFakeWSConn()
	handleAuthorization(Envelope{Code: 1}, svc)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if !svc.isAuth || svc.authFailures != 0 || svc.reconnectAttempts != 0 {
		t.Fatalf("isAuth=%v authFailures=%d attempts=%d", svc.isAuth, svc.authFailures, svc.reconnectAttempts)
	}
}

func TestAuthDeadlineClosesUnauthorizedSocket(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.authTimeout = 20 * time.Millisecond
	conn := newFakeWSConn()
	svc.conn = conn
	svc.isOpen = true
	done := make(chan struct{})
	defer close(done)
	svc.watchAuthDeadline(conn, done)
	if !conn.isClosed() {
		t.Fatal("unauthorized socket was not closed")
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if !svc.authFailed {
		t.Fatal("deadline should mark the socket for slow retry")
	}
}

func TestAuthDeadlineLeavesAuthorizedSocket(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.authTimeout = 10 * time.Millisecond
	conn := newFakeWSConn()
	svc.conn = conn
	svc.isOpen = true
	svc.isAuth = true
	done := make(chan struct{})
	defer close(done)
	svc.watchAuthDeadline(conn, done)
	if conn.isClosed() {
		t.Fatal("authorized socket was closed")
	}
}

type countingHTTPDoer struct {
	mu    stdsync.Mutex
	calls int
}

func (c *countingHTTPDoer) Do(*http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(""))}, nil
}

func (c *countingHTTPDoer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// The reconnect loop must keep going well past reconnectMaxAttempts.
func TestReconnectNeverGivesUp(t *testing.T) {
	svc := newTestService(nil, nil, "")
	doer := &countingHTTPDoer{}
	svc.httpDoer = doer
	svc.isRegister = true
	go svc.connectOnce()
	deadline := time.Now().Add(5 * time.Second)
	for doer.count() < 3*reconnectMaxAttempts && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	_ = svc.Close()
	if n := doer.count(); n < 3*reconnectMaxAttempts {
		t.Fatalf("only %d attempts before giving up", n)
	}
}

func TestForceReconnectCutsBackoffShort(t *testing.T) {
	svc := newTestService(nil, nil, "")
	release := make(chan struct{})
	defer close(release)
	svc.sleepFn = func(time.Duration) { <-release }
	got := make(chan bool, 1)
	go func() { got <- svc.waitReconnect(time.Hour) }()
	time.Sleep(10 * time.Millisecond)
	svc.forceReconnect("test")
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("kicked wait should proceed to reconnect")
		}
	case <-time.After(time.Second):
		t.Fatal("forceReconnect did not cut the wait short")
	}
}

func TestWatchdogForcesReconnectThenFails(t *testing.T) {
	svc := newTestService(nil, nil, "")
	conn := newFakeWSConn()
	svc.conn = conn
	svc.isOpen = true
	svc.isAuth = true
	svc.isRegister = true
	now := time.Now()
	svc.lastHealthy = now.Add(-watchdogIdle - time.Second)

	svc.checkWedged(now)
	if !conn.isClosed() {
		t.Fatal("first trip should force a reconnect")
	}
	select {
	case err := <-svc.Fatal():
		t.Fatalf("first trip should not be fatal: %v", err)
	default:
	}

	// Still nothing after another idle window.
	svc.checkWedged(now.Add(watchdogIdle + time.Second))
	select {
	case err := <-svc.Fatal():
		if err == nil {
			t.Fatal("nil fatal error")
		}
	default:
		t.Fatal("second trip should request exit")
	}
}

func TestWatchdogResetByCompletedRound(t *testing.T) {
	svc := newTestService(nil, nil, filepath.Join(t.TempDir(), "state.json"))
	svc.isRegister = true
	now := time.Now()
	svc.lastHealthy = now.Add(-watchdogIdle - time.Second)
	svc.checkWedged(now)      // trip once
	if !svc.watchdogTripped { // no goroutines: direct read is safe
		t.Fatal("expected trip")
	}
	svc.isSyncing = true
	svc.syncRoundID = 3
	svc.onSyncComplete(3, true)
	svc.mu.Lock()
	tripped := svc.watchdogTripped
	last := svc.lastHealthy
	svc.mu.Unlock()
	if tripped {
		t.Fatal("completed round should reset the watchdog")
	}
	svc.checkWedged(last.Add(watchdogIdle - time.Second))
	select {
	case err := <-svc.Fatal():
		t.Fatalf("healthy client marked fatal: %v", err)
	default:
	}
}

func TestWatchdogIgnoresUnregistered(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.lastHealthy = time.Now().Add(-time.Hour)
	svc.checkWedged(time.Now())
	if svc.watchdogTripped {
		t.Fatal("watchdog acted on a stopped service")
	}
}

func TestWatchdogManualModeTreatsAuthAsHealthy(t *testing.T) {
	svc := newTestService(&config.Config{API: "http://x", ManualSyncEnabled: true}, nil, "")
	conn := newFakeWSConn()
	svc.conn = conn
	svc.isRegister = true
	svc.isOpen = true
	svc.isAuth = true
	svc.lastHealthy = time.Now().Add(-time.Hour)
	svc.checkWedged(time.Now())
	if conn.isClosed() || svc.watchdogTripped {
		t.Fatal("manual mode with an authorized socket is healthy")
	}
}

func TestClockJump(t *testing.T) {
	t0 := time.Date(2026, 10, 22, 8, 18, 54, 0, time.UTC)
	if j := clockJump(t0, t0.Add(5*time.Second), 5*time.Second); j != 0 {
		t.Fatalf("no suspend: jump = %v", j)
	}
	// Host paused for ~20 minutes: wall clock advanced, monotonic did not.
	if j := clockJump(t0, t0.Add(20*time.Minute+5*time.Second), 5*time.Second); j != 20*time.Minute {
		t.Fatalf("suspend: jump = %v", j)
	}
}

// The watchdog loop reconnects promptly after a suspend instead of waiting
// for the read deadline and periodic ticker.
func TestWatchdogLoopReconnectsAfterSuspend(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.watchdogEvery = 5 * time.Millisecond
	conn := newFakeWSConn()
	svc.mu.Lock()
	svc.conn = conn
	svc.isRegister = true
	svc.lastHealthy = time.Now()
	svc.mu.Unlock()
	var calls int
	var mu stdsync.Mutex
	svc.clockJumpFn = func(prevWall, nowWall time.Time, mono time.Duration) time.Duration {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 3 {
			return 20 * time.Minute
		}
		return 0
	}
	go svc.watchdogLoop()
	defer func() { _ = svc.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for !conn.isClosed() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !conn.isClosed() {
		t.Fatal("suspend was not detected")
	}
}

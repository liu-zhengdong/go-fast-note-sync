package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

const (
	reconnectBaseDelay   = 1000 * time.Millisecond
	reconnectMaxDelay    = 60 * time.Second
	reconnectMaxAttempts = 15
	healthCheckTimeout   = 10 * time.Second

	wsTextMessage   = gorillaws.TextMessage
	wsBinaryMessage = gorillaws.BinaryMessage

	binaryPrefixFileSync = "00"
)

// Keepalive timing vars; var (not const) so tests can override.
//
// The service pings every 25s and closes a client that does not pong within
// 60s (WSPingWait). Proxies on the path often use the same 60s idle timeout
// and do not always treat an unanswered control frame as traffic. A 20s client
// ping stays inside both windows with margin for a data write that is holding
// the socket lock.
var (
	pingInterval = 20 * time.Second
	pongWait     = 70 * time.Second // read deadline; refreshed by ping and pong
	writeWait    = 10 * time.Second // write deadline for data frames and pongs

	// periodicSyncInterval is how often the daemon reconciles even when the
	// server does not push ClientInfo. The service only broadcasts ClientInfo
	// from its 10-minute version check, and only when it considers the client
	// outdated, so that push is not a sync schedule.
	periodicSyncInterval = 10 * time.Minute

	// slotAcquireTimeout bounds WaitForSlot. The previous implementation blocked
	// forever, which pinned the read loop or a watcher callback and made
	// SIGTERM look ignored.
	slotAcquireTimeout = 60 * time.Second

	// defaultSyncIdleTimeout matches config.SyncTimeoutSeconds == 0. It is an
	// idle timeout: a round that is still hashing files or receiving pages
	// does not expire.
	defaultSyncIdleTimeout = 60 * time.Second

	// slotLeaseTimeout reclaims a concurrency slot whose Ack never arrived.
	slotLeaseTimeout = 3 * time.Minute
	slotPollInterval = time.Second

	// authHandshakeTimeout closes a socket that connected but was never
	// authorized. Without it a lost or unanswered Authorization reply leaves
	// the client connected, unauthenticated and silent indefinitely: the
	// periodic reconciler only runs once isAuth is set.
	authHandshakeTimeout = 30 * time.Second

	// Slow backoff after the server rejects the token or closes for policy
	// reasons. These used to stop reconnecting for good, so one transient
	// rejection (seen: Code=315 "Auth token Scope restricted") left the
	// daemon idle until an external restart.
	authRetryBaseDelay = 30 * time.Second
	authRetryMaxDelay  = 10 * time.Minute

	// watchdogTick is how often the in-process watchdog samples liveness.
	watchdogTick = 5 * time.Second
	// watchdogIdle is how long the client may go without a completed sync
	// round or any sync progress before the watchdog forces a reconnect. A
	// second idle window after that exits the process so the supervisor
	// restarts it. Measured on the monotonic clock, so time the host spends
	// suspended does not count.
	watchdogIdle = 15 * time.Minute
	// suspendJumpThreshold: if wall-clock time advances this much more than
	// monotonic time between two watchdog ticks, the VM/host was paused. The
	// socket is almost certainly dead by then, so reconnect immediately
	// instead of waiting out the read deadline and the periodic ticker.
	suspendJumpThreshold = 30 * time.Second
)

// slowReconnectCloseReasons are server close reasons that mean "not now"
// (bad/rotated token, kicked, policy). The client keeps retrying them with
// authRetryDelay instead of giving up.
var slowReconnectCloseReasons = map[string]struct{}{
	"AuthorizationFaild":    {},
	"ClientClose":           {},
	"kicked by admin":       {},
	"TokenRotatedOrRevoked": {},
	"broadcast failed":      {},
}

// WSConn abstracts a WebSocket connection for testability.
type WSConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetPongHandler(h func(appData string) error)
	SetPingHandler(h func(appData string) error)
	Close() error
}

// Dialer abstracts WebSocket dialing for testability.
type Dialer interface {
	Dial(urlStr string, header http.Header) (WSConn, *http.Response, error)
}

// HTTPDoer abstracts HTTP client requests for testability.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// gorillaDialer wraps gorilla/websocket.Dialer to implement Dialer.
type gorillaDialer struct{}

func (g *gorillaDialer) Dial(urlStr string, header http.Header) (WSConn, *http.Response, error) {
	conn, resp, err := wsDialer.Dial(urlStr, header)
	if err != nil {
		return nil, resp, err
	}
	return conn, resp, nil
}

// wsDialer enables TCP keepalives. A half-open connection otherwise sits in
// the kernel until the retransmission timeout, which is long enough to look
// like a multi-tens-of-minutes silence when the websocket read is also stuck.
var wsDialer = &gorillaws.Dialer{
	Proxy:            http.ProxyFromEnvironment,
	HandshakeTimeout: 45 * time.Second,
	NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
		return d.DialContext(ctx, network, addr)
	},
}

// SyncTaskCounter tracks per-module sync task progress.
type SyncTaskCounter struct {
	NeedUpload    int
	NeedModify    int
	NeedDelete    int
	NeedSyncMtime int
	Completed     int
}

type syncPage struct {
	TotalCount     int
	CompletedCount int
	IsLast         bool
}

type syncPageTracker struct {
	Context      string
	AckWatermark int
	Pages        map[int]*syncPage
}

// SyncService manages the WebSocket connection lifecycle and sync state.
type SyncService struct {
	cfg       *config.Config
	st        *state.State
	statePath string
	version   string

	runAPI string // may be updated when AutoRedirectEnabled follows a redirect

	dialer   Dialer
	httpDoer HTTPDoer

	mu         stdsync.Mutex
	conn       WSConn
	isOpen     bool
	isAuth     bool
	isRegister bool

	isSyncing             bool
	isSyncRequesting      bool
	syncRoundID           uint64
	syncRoundConn         WSConn
	syncActivity          atomic.Uint64
	overflowRescanPending bool
	debugDisconnectAfter  time.Duration
	debugDisconnectOnce   stdsync.Once

	// beforeCommitScan, when set, runs after a scan decides it still owns the
	// round and before that result is published. Tests use it to disconnect
	// in that window.
	beforeCommitScan func(roundID uint64)

	stopCh       chan struct{}
	stopOnce     stdsync.Once
	periodicOnce stdsync.Once
	watchdogOnce stdsync.Once

	// reconnect bookkeeping
	reconnectAttempts int  // attempts since the last successful auth (for logs)
	authFailures      int  // consecutive auth/policy rejections
	authFailed        bool // the current socket got an auth rejection
	kickCh            chan struct{}

	// watchdog state (guarded by mu)
	lastHealthy     time.Time
	watchdogTripped bool
	fatalCh         chan error
	fatalOnce       stdsync.Once
	clockJumpFn     func(prevWall, nowWall time.Time, monoElapsed time.Duration) time.Duration // tests
	authTimeout     time.Duration                                                              // tests; 0 = authHandshakeTimeout
	watchdogEvery   time.Duration                                                              // tests; 0 = watchdogTick
	// periodicInterval overrides periodicSyncInterval in tests. Zero uses the
	// package default.
	periodicInterval time.Duration

	binaryHandlers  map[string]func([]byte)
	receiveHandlers map[string]func(json.RawMessage, *SyncService)

	timeConnect int
	sleepFn     func(time.Duration)

	// configurable for testing
	syncTimeout     time.Duration
	folderWaitPoll  time.Duration
	folderWaitLimit time.Duration

	// pending hash maps (mirrored to State for crash recovery)
	pendingNoteModifies   map[string]string
	pendingUploadHashes   map[string]string
	pendingConfigModifies map[string]string

	// Round-scoped bookkeeping so a dropped round never loses or redoes work.
	// roundSyncTimes holds lastTime values seen during an in-flight round; they
	// are only persisted when the round completes, so a reconnect cannot skip
	// server pages that were never processed. roundNoteSent/roundNoteAddressed
	// let a completed round commit notes the server left untouched (already in
	// sync), instead of keeping them pending and re-sending them forever.
	roundSyncTimes     map[string]int64
	roundNoteSent      map[string]string
	roundNoteAddressed map[string]struct{}

	// pending delete sets (cleared after corresponding SyncEnd)
	pendingNoteDeleteAcks    map[string]struct{}
	pendingFileDeleteAcks    map[string]struct{}
	pendingConfigDeleteAcks  map[string]struct{}
	pendingDeleteNotePaths   map[string]struct{}
	pendingDeleteFilePaths   map[string]struct{}
	pendingDeleteFolderPaths map[string]struct{}
	pendingDeleteConfigPaths map[string]struct{}

	// rename FIFO queues (TCP ordering guarantees correct Ack pairing)
	pendingNoteRenames []struct{ OldPath, NewPath, ContentHash string }
	pendingFileRenames []struct{ OldPath, NewPath, ContentHash string }

	fileDownloadSessions map[string]*FileDownloadSession
	activeUploads        map[string]*ActiveUpload
	TempChunksBaseDir    string
	writeMu              stdsync.Mutex

	// sync task counters
	noteSyncTasks   SyncTaskCounter
	fileSyncTasks   SyncTaskCounter
	configSyncTasks SyncTaskCounter
	folderSyncTasks SyncTaskCounter

	// sync phase flags
	noteSyncEnd   bool
	fileSyncEnd   bool
	configSyncEnd bool
	folderSyncEnd bool
	syncPages     map[string]*syncPageTracker

	// echo suppression / scan caches
	lastSyncMtime       map[string]int64
	lastSyncPathDeleted map[string]struct{}
	lastSyncPathRenamed map[string]struct{}
	scannedNoteHashes   map[string]state.FileHashEntry
	scannedFileHashes   map[string]state.FileHashEntry
	scannedConfigHashes map[string]state.FileHashEntry

	concurrency *ConcurrencyManager
	pathLocks   map[string]chan struct{}

	pingWG stdsync.WaitGroup

	syncDoneCh   chan struct{}
	syncDoneOnce stdsync.Once
	syncErr      error
}

// NewSyncService creates a SyncService with production defaults.
func NewSyncService(cfg *config.Config, st *state.State, statePath, version string) *SyncService {
	s := &SyncService{
		cfg:             cfg,
		st:              st,
		statePath:       statePath,
		version:         version,
		runAPI:          cfg.API,
		dialer:          &gorillaDialer{},
		httpDoer:        http.DefaultClient,
		binaryHandlers:  make(map[string]func([]byte)),
		receiveHandlers: buildReceiveHandlers(),
		sleepFn:         time.Sleep,

		pendingNoteModifies:      copyStringMap(st.PendingNoteModifies),
		pendingUploadHashes:      copyStringMap(st.PendingUploadHashes),
		pendingConfigModifies:    copyStringMap(st.PendingConfigModifies),
		pendingNoteDeleteAcks:    make(map[string]struct{}),
		pendingFileDeleteAcks:    make(map[string]struct{}),
		pendingConfigDeleteAcks:  make(map[string]struct{}),
		pendingDeleteNotePaths:   make(map[string]struct{}),
		pendingDeleteFilePaths:   make(map[string]struct{}),
		pendingDeleteFolderPaths: make(map[string]struct{}),
		pendingDeleteConfigPaths: make(map[string]struct{}),
		fileDownloadSessions:     make(map[string]*FileDownloadSession),
		activeUploads:            make(map[string]*ActiveUpload),
		TempChunksBaseDir:        tempChunksBaseDir(statePath),
		lastSyncMtime:            make(map[string]int64),
		lastSyncPathDeleted:      make(map[string]struct{}),
		lastSyncPathRenamed:      make(map[string]struct{}),
		scannedNoteHashes:        make(map[string]state.FileHashEntry),
		scannedFileHashes:        make(map[string]state.FileHashEntry),
		scannedConfigHashes:      make(map[string]state.FileHashEntry),
		concurrency:              NewConcurrencyManager(cfg),
		pathLocks:                make(map[string]chan struct{}),
		syncPages:                make(map[string]*syncPageTracker),
		syncDoneCh:               make(chan struct{}),
		stopCh:                   make(chan struct{}),
		kickCh:                   make(chan struct{}, 1),
		fatalCh:                  make(chan error, 1),
	}
	if cfg.SyncTimeoutSeconds > 0 {
		s.syncTimeout = time.Duration(cfg.SyncTimeoutSeconds) * time.Second
	}
	s.binaryHandlers[binaryPrefixFileSync] = s.handleFileBinaryChunk
	return s
}

// SyncComplete returns a channel that is closed once the first sync round completes.
func (s *SyncService) SyncComplete() <-chan struct{} {
	return s.syncDoneCh
}

// Fatal delivers an error when the watchdog decides the process is wedged
// beyond in-process recovery. The daemon should exit non-zero so its
// supervisor restarts it.
func (s *SyncService) Fatal() <-chan error {
	return s.fatalCh
}

func (s *SyncService) fail(err error) {
	s.fatalOnce.Do(func() {
		log.Printf("[watchdog] fatal: %v", err)
		if s.fatalCh != nil {
			s.fatalCh <- err
		}
	})
}

func (s *SyncService) SyncError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncErr
}

// SetDebugDisconnectAfter arms a one-shot debug hook that closes the active
// WebSocket after the given duration following a successful dial. Zero disables
// the hook. Intended for deterministic reconnect smoke; not a production flag.
func (s *SyncService) SetDebugDisconnectAfter(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.debugDisconnectAfter = d
	s.debugDisconnectOnce = stdsync.Once{}
}

func (s *SyncService) scheduleDebugDisconnect(conn WSConn) {
	s.mu.Lock()
	after := s.debugDisconnectAfter
	s.mu.Unlock()
	if after <= 0 || conn == nil {
		return
	}
	s.debugDisconnectOnce.Do(func() {
		go func() {
			time.Sleep(after)
			s.mu.Lock()
			current := s.conn
			registered := s.isRegister
			s.mu.Unlock()
			if !registered || current == nil || current != conn {
				return
			}
			log.Printf("[debug] closing connection after %v for reconnect smoke", after)
			_ = conn.Close()
		}()
	})
}

// buildWSURL constructs the WebSocket connection URL.
// count is the value of State.WsCount used as the query parameter.
// clientType is used for the `client` query parameter; falls back to
// config.DefaultClientType when empty.
func buildWSURL(apiURL, clientType, version string, count int) (string, error) {
	var wsBase string
	switch {
	case strings.HasPrefix(apiURL, "https://"):
		wsBase = "wss://" + apiURL[8:]
	case strings.HasPrefix(apiURL, "http://"):
		wsBase = "ws://" + apiURL[7:]
	default:
		return "", fmt.Errorf("API URL must start with http:// or https://, got %q", apiURL)
	}
	wsBase = strings.TrimRight(wsBase, "/")

	if clientType == "" {
		clientType = config.DefaultClientType
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return fmt.Sprintf(
		"%s/api/user/sync?lang=%s&count=%d&client=%s&clientName=%s&clientVersion=%s",
		wsBase,
		systemLocale(),
		count,
		url.QueryEscape(clientType),
		url.QueryEscape(hostname),
		url.QueryEscape(version),
	), nil
}

func systemLocale() string {
	for _, env := range []string{"LANG", "LANGUAGE", "LC_ALL"} {
		v := os.Getenv(env)
		if v == "" {
			continue
		}
		if idx := strings.IndexAny(v, "_."); idx != -1 {
			v = v[:idx]
		}
		if v != "" {
			return v
		}
	}
	return "en"
}

// reconnectDelay returns the backoff delay for the n-th reconnect attempt (1-based).
// Formula: 1s, 1s, 1s, 2s, 4s..., capped at reconnectMaxDelay.
// withJitter adds up to +25% random jitter so many clients (or rapid drops)
// do not reconnect in lockstep.
func withJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration(rand.Int63n(int64(d)/4+1))
}

func reconnectDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt <= 3 {
		return reconnectBaseDelay
	}
	shift := attempt - 3
	if shift > 30 {
		shift = 30
	}
	delay := reconnectBaseDelay * (1 << uint(shift))
	if delay > reconnectMaxDelay {
		return reconnectMaxDelay
	}
	return delay
}

// authRetryDelay is the slow backoff after an auth/policy rejection:
// 30s, 1m, 2m, 4m, 8m, then 10m.
func authRetryDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	shift := failures - 1
	if shift > 20 {
		shift = 20
	}
	d := authRetryBaseDelay * (1 << uint(shift))
	if d > authRetryMaxDelay || d <= 0 {
		return authRetryMaxDelay
	}
	return d
}

// Connect starts the connection lifecycle, the periodic reconciler and the
// watchdog. Both loops are tied to the process, not to one socket, so a
// reconnect does not cancel the next round.
func (s *SyncService) Connect() {
	s.mu.Lock()
	s.isRegister = true
	s.lastHealthy = time.Now()
	s.mu.Unlock()
	s.periodicOnce.Do(func() { go s.periodicSyncLoop() })
	s.watchdogOnce.Do(func() { go s.watchdogLoop() })
	go s.connectOnce()
}

// markHealthy records that sync is making progress.
func (s *SyncService) markHealthy() {
	s.mu.Lock()
	s.lastHealthy = time.Now()
	s.watchdogTripped = false
	s.mu.Unlock()
}

// forceReconnect drops the current socket (readLoop then unwinds into the
// normal reconnect path, which also abandons the in-flight round and frees
// concurrency slots) or, when no socket is up, cuts the backoff wait short.
func (s *SyncService) forceReconnect(reason string) {
	s.mu.Lock()
	conn := s.conn
	s.timeConnect = 0
	s.mu.Unlock()
	if conn != nil {
		log.Printf("[ws] forcing reconnect (%s)", reason)
		_ = conn.Close()
		return
	}
	if s.kickCh != nil {
		select {
		case s.kickCh <- struct{}{}:
		default:
		}
	}
}

// watchdogLoop is the in-process liveness monitor. See watchdogIdle and
// suspendJumpThreshold.
func (s *SyncService) watchdogLoop() {
	every := s.watchdogEvery
	if every <= 0 {
		every = watchdogTick
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	lastMono := time.Now()
	lastWall := lastMono.Round(0)
	lastProgress := s.syncProgressSnapshot()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
		nowMono := time.Now()
		nowWall := nowMono.Round(0) // strip monotonic reading: wall-clock delta
		jumpFn := s.clockJumpFn
		if jumpFn == nil {
			jumpFn = clockJump
		}
		jump := jumpFn(lastWall, nowWall, nowMono.Sub(lastMono))
		lastMono, lastWall = nowMono, nowWall
		if jump > suspendJumpThreshold {
			log.Printf("[watchdog] wall clock moved %v more than monotonic time (host was suspended); reconnecting now", jump.Round(time.Second))
			s.forceReconnect("resume after suspend")
			continue
		}
		if p := s.syncProgressSnapshot(); p != lastProgress {
			lastProgress = p
			s.markHealthy()
		}
		s.checkWedged(nowMono)
	}
}

// clockJump returns how much further the wall clock moved than the monotonic
// clock. Linux CLOCK_MONOTONIC does not advance while a VM is paused, but the
// wall clock is corrected on resume, so a large positive value means the host
// was suspended.
func clockJump(prevWall, nowWall time.Time, monoElapsed time.Duration) time.Duration {
	return nowWall.Round(0).Sub(prevWall.Round(0)) - monoElapsed
}

// checkWedged trips the watchdog when no sync round has completed and no
// sync progress was seen for watchdogIdle: first by forcing a reconnect,
// then, if a further idle window passes, by asking the process to exit.
func (s *SyncService) checkWedged(now time.Time) {
	s.mu.Lock()
	if !s.isRegister || s.lastHealthy.IsZero() {
		s.mu.Unlock()
		return
	}
	if s.cfg != nil && s.cfg.ManualSyncEnabled && s.isOpen && s.isAuth {
		// Manual mode never runs rounds on its own; an authorized socket is
		// the best available liveness signal.
		s.lastHealthy = now
		s.watchdogTripped = false
		s.mu.Unlock()
		return
	}
	idle := now.Sub(s.lastHealthy)
	if idle < watchdogIdle {
		s.mu.Unlock()
		return
	}
	tripped := s.watchdogTripped
	s.watchdogTripped = true
	s.lastHealthy = now
	s.mu.Unlock()
	if !tripped {
		log.Printf("[watchdog] no completed sync round or sync progress for %v; forcing reconnect", idle.Round(time.Second))
		s.forceReconnect("watchdog")
		return
	}
	s.fail(fmt.Errorf("no completed sync round for %v after a forced reconnect", idle.Round(time.Second)))
}

// Close stops reconnects, abandons any in-flight round, and closes the socket
// so a blocked read or watcher callback can return. Safe to call more than once.
func (s *SyncService) Close() error {
	s.mu.Lock()
	s.isRegister = false
	s.abandonActiveSyncLocked("shutdown")
	s.isOpen = false
	s.isAuth = false
	conn := s.conn
	s.mu.Unlock()
	if s.concurrency != nil {
		s.concurrency.Stop()
	}
	s.closeStop()
	if conn != nil {
		_ = conn.Close()
	}
	return nil
}

func (s *SyncService) closeStop() {
	s.stopOnce.Do(func() {
		if s.stopCh == nil {
			s.stopCh = make(chan struct{})
		}
		close(s.stopCh)
	})
}

func (s *SyncService) periodicSyncLoop() {
	interval := s.periodicInterval
	if interval <= 0 {
		interval = periodicSyncInterval
	}
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.mu.Lock()
			ready := s.isRegister && s.isOpen && s.isAuth && s.cfg != nil && !s.cfg.ManualSyncEnabled
			loadLast := s.st != nil && s.st.IsInitSync
			s.mu.Unlock()
			if !ready {
				continue
			}
			log.Printf("[sync] periodic sync")
			s.handleSync(loadLast)
		}
	}
}

// connectOnce performs one full connection attempt: health check → dial → read loop.
func (s *SyncService) connectOnce() {
	healthy, newAPI := s.checkHealth()
	if newAPI != "" {
		s.runAPI = newAPI
	}
	if !healthy {
		log.Printf("[ws] health check failed, scheduling reconnect")
		s.scheduleReconnect()
		return
	}

	wsURL, err := buildWSURL(s.runAPI, s.cfg.ClientType, s.version, s.st.WsCount)
	if err != nil {
		log.Printf("[ws] build URL: %v", err)
		s.scheduleReconnect()
		return
	}

	conn, _, err := s.dialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("[ws] dial: %v", err)
		s.scheduleReconnect()
		return
	}

	// Increment and persist WsCount after a successful dial.
	s.st.WsCount++
	if saveErr := state.Save(s.statePath, s.st); saveErr != nil {
		log.Printf("[ws] save state after connect: %v", saveErr)
	}

	// Set up keepalive on the local conn before handing it to readLoop.
	if err := installKeepalive(conn); err != nil {
		log.Printf("[ws] set read deadline: %v", err)
	}
	done := make(chan struct{})
	s.pingWG.Add(1)
	go s.pingLoop(conn, done)

	s.mu.Lock()
	s.conn = conn
	s.isOpen = true
	s.isAuth = false
	s.timeConnect = 0
	s.mu.Unlock()

	log.Printf("[ws] connected (wsCount=%d)", s.st.WsCount)
	s.scheduleDebugDisconnect(conn)
	if err := s.Send("Authorization", s.cfg.APIToken); err != nil {
		log.Printf("[ws] failed to send Authorization: %v", err)
	}
	go s.watchAuthDeadline(conn, done)

	closeReason := s.readLoop()
	close(done)

	s.mu.Lock()
	s.isOpen = false
	s.isAuth = false
	// Bump the round id as well as the flag. Clearing the bool alone lets a
	// scan that already passed its ownership check publish isSyncRequesting
	// again after the next round has moved on, and every later ClientInfo
	// then logs "already syncing" until the process is restarted.
	s.abandonActiveSyncLocked("disconnect")
	s.mu.Unlock()
	// Acks for in-flight pushes on the dead socket will never arrive; free
	// their slots so the next round is not starved ("concurrency slot
	// unavailable"). The paths stay pending and are re-sent next round.
	s.concurrency.Clear()

	s.mu.Lock()
	slow := s.authFailed || slowReconnectCloseReason(closeReason)
	s.authFailed = false
	shouldReconnect := s.isRegister
	s.mu.Unlock()
	if !shouldReconnect {
		return
	}
	if slow {
		log.Printf("[ws] server refused the session (reason=%q); retrying with slow backoff", closeReason)
	}
	s.scheduleReconnectWith(slow)
}

// watchAuthDeadline closes conn if it is still unauthorized after
// authHandshakeTimeout.
func (s *SyncService) watchAuthDeadline(conn WSConn, done <-chan struct{}) {
	timeout := s.authTimeout
	if timeout <= 0 {
		timeout = authHandshakeTimeout
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-done:
		return
	case <-s.stopCh:
		return
	case <-t.C:
	}
	s.mu.Lock()
	stale := s.conn == conn && s.isOpen && !s.isAuth
	if stale {
		s.authFailed = true
	}
	s.mu.Unlock()
	if stale {
		log.Printf("[ws] not authorized within %v; closing connection", timeout)
		_ = conn.Close()
	}
}

// onAuthRejected closes the socket after the server rejected Authorization so
// the reconnect loop retries with slow backoff, instead of sitting on a
// connected but unauthenticated socket.
func (s *SyncService) onAuthRejected() {
	s.mu.Lock()
	s.authFailed = true
	conn := s.conn
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// onAuthSucceeded resets backoff state after a successful Authorization.
func (s *SyncService) onAuthSucceeded() {
	s.mu.Lock()
	s.isAuth = true
	s.authFailures = 0
	s.authFailed = false
	s.reconnectAttempts = 0
	s.mu.Unlock()
}

// checkHealth performs GET /api/health.
// Returns (healthy, newRuntimeAPI). newRuntimeAPI is non-empty only when
// AutoRedirectEnabled detects a redirect, signalling the caller to update runAPI.
func (s *SyncService) checkHealth() (bool, string) {
	base := strings.TrimRight(s.runAPI, "/")
	healthURL := base + "/api/health"
	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()

	if s.cfg.AutoRedirectEnabled {
		var redirectURL string
		client := &http.Client{
			Timeout: healthCheckTimeout,
			CheckRedirect: func(req *http.Request, _ []*http.Request) error {
				redirectURL = req.URL.String()
				return nil
			},
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return false, ""
		}
		resp, err := client.Do(req)
		if err != nil {
			return false, ""
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()

		var newAPI string
		if redirectURL != "" {
			if u, parseErr := url.Parse(redirectURL); parseErr == nil {
				newAPI = u.Scheme + "://" + u.Host
			}
		}
		return resp.StatusCode/100 == 2 || resp.StatusCode == 404, newAPI
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return false, ""
	}
	resp, err := s.httpDoer.Do(req)
	if err != nil {
		return false, ""
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp.StatusCode/100 == 2 || resp.StatusCode == 404, ""
}

// readLoop reads messages from the connection until it closes.
// Returns the WebSocket close reason string.
func (s *SyncService) readLoop() string {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.conn != nil {
			s.conn.Close() //nolint:errcheck
			s.conn = nil
		}
		s.mu.Unlock()
	}()

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			reason := extractWSCloseReason(err)
			log.Printf("[ws] read closed: %v", err)
			return reason
		}
		switch msgType {
		case wsTextMessage:
			s.dispatchText(string(data))
		case wsBinaryMessage:
			s.dispatchBinary(data)
		}
	}
}

func extractWSCloseReason(err error) string {
	var ce *gorillaws.CloseError
	if errors.As(err, &ce) {
		return ce.Text
	}
	return ""
}

func slowReconnectCloseReason(reason string) bool {
	_, ok := slowReconnectCloseReasons[reason]
	return ok
}

// scheduleReconnect waits for the exponential backoff delay then retries.
func (s *SyncService) scheduleReconnect() {
	s.scheduleReconnectWith(false)
}

// scheduleReconnectWith waits for the backoff delay then starts the next
// attempt. It never gives up: network failures back off to reconnectMaxDelay,
// auth/policy rejections (slow=true) to authRetryMaxDelay. The next attempt
// runs on a fresh goroutine so a long-lived daemon does not grow one stack
// frame pair per reconnect.
func (s *SyncService) scheduleReconnectWith(slow bool) {
	s.mu.Lock()
	var delay time.Duration
	if slow {
		s.authFailures++
		delay = authRetryDelay(s.authFailures)
	} else {
		s.timeConnect++
		if s.timeConnect > reconnectMaxAttempts {
			s.timeConnect = reconnectMaxAttempts
		}
		delay = reconnectDelay(s.timeConnect)
	}
	s.reconnectAttempts++
	attempt := s.reconnectAttempts
	s.mu.Unlock()
	delay = withJitter(delay)
	log.Printf("[ws] reconnecting in %v (attempt %d)", delay.Round(time.Millisecond), attempt)
	if !s.waitReconnect(delay) {
		log.Printf("[ws] reconnect cancelled by shutdown")
		return
	}
	s.mu.Lock()
	shouldReconnect := s.isRegister
	s.mu.Unlock()
	if shouldReconnect {
		go s.connectOnce()
	}
}

// waitReconnect pauses before the next dial. Closing stopCh (SIGTERM/SIGINT)
// returns immediately so a long backoff cannot keep the process alive.
func (s *SyncService) waitReconnect(delay time.Duration) bool {
	done := make(chan struct{})
	go func() {
		if s.sleepFn != nil {
			s.sleepFn(delay)
		} else {
			time.Sleep(delay)
		}
		close(done)
	}()
	select {
	case <-s.stopCh:
		return false
	case <-done:
		return true
	case <-s.kickCh:
		log.Printf("[ws] reconnect wait cut short")
		return true
	}
}

// installKeepalive arms the read deadline and ping/pong handlers.
//
// gorilla's default ping handler writes the pong with a 1s deadline while the
// read loop is inside ReadMessage. A data write that holds the socket lock
// longer than that — a multi-megabyte NoteSync easily can — fails the pong,
// fails the read, and the service then closes the TCP connection with no
// close frame (client log: close 1006 unexpected EOF) because its own ping
// was not answered within 60s. This handler uses writeWait and treats an
// incoming ping as liveness, same as a pong.
func installKeepalive(conn WSConn) error {
	if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		return err
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	conn.SetPingHandler(func(appData string) error {
		if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			return err
		}
		err := conn.WriteControl(gorillaws.PongMessage, []byte(appData), time.Now().Add(writeWait))
		if err == nil || errors.Is(err, gorillaws.ErrCloseSent) {
			return nil
		}
		log.Printf("[ws] pong write: %v", err)
		return err
	})
	return nil
}

// pingLoop sends periodic WebSocket ping control frames until done is closed.
// It uses WriteControl (not WriteMessage) to avoid contending with the write mutex.
func (s *SyncService) pingLoop(conn WSConn, done <-chan struct{}) {
	defer s.pingWG.Done()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := conn.WriteControl(gorillaws.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				log.Printf("[ws] ping write: %v", err)
			}
		}
	}
}

// Send writes a text frame formatted as "ACTION|payload".
func (s *SyncService) Send(action string, payload interface{}) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()

	if conn == nil {
		err := fmt.Errorf("not connected")
		log.Printf("[ws] send: %v (action=%s)", err, action)
		return err
	}

	var body string
	switch v := payload.(type) {
	case string:
		body = v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			log.Printf("[ws] send marshal: %v", err)
			return err
		}
		body = string(b)
	}

	s.writeMu.Lock()
	err := s.writeWithDeadline(conn, wsTextMessage, []byte(action+"|"+body))
	s.writeMu.Unlock()
	if err != nil {
		log.Printf("[ws] send %s: %v", action, err)
		return err
	}
	return nil
}

// writeWithDeadline bounds a data-frame write with writeWait, mirroring the
// deadline already applied to ping control frames. Without it, a write to a
// peer that has stopped reading blocks until the OS abandons the TCP
// connection — holding writeMu, and with it the entire sync round, for hours.
func (s *SyncService) writeWithDeadline(conn WSConn, messageType int, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return conn.WriteMessage(messageType, data)
}

// SendBinary writes a binary frame with a 2-byte ASCII prefix prepended.
func (s *SyncService) SendBinary(prefix string, data []byte) error {
	if len(prefix) != 2 {
		return fmt.Errorf("binary prefix must be exactly 2 characters, got %q", prefix)
	}
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	frame := make([]byte, 2+len(data))
	copy(frame[:2], prefix)
	copy(frame[2:], data)
	s.writeMu.Lock()
	err := s.writeWithDeadline(conn, wsBinaryMessage, frame)
	s.writeMu.Unlock()
	return err
}

// dispatchBinary routes a binary message to the handler registered for its 2-byte prefix.
func (s *SyncService) dispatchBinary(data []byte) {
	if len(data) < 2 {
		log.Printf("[ws] binary message too short: %d bytes", len(data))
		return
	}
	prefix := string(data[:2])
	handler, ok := s.binaryHandlers[prefix]
	if !ok {
		log.Printf("[ws] no binary handler for prefix %q", prefix)
		return
	}
	handler(data[2:])
}

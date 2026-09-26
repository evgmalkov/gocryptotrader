package mexc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrasher-corp/gocryptotrader/config"
	"github.com/thrasher-corp/gocryptotrader/encoding/json"
	"github.com/thrasher-corp/gocryptotrader/exchange/accounts"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/sharedtestvalues"
	"github.com/thrasher-corp/gocryptotrader/exchanges/subscription"
	testutils "github.com/thrasher-corp/gocryptotrader/internal/testing/utils"
)

// wsLifecycleMonitorDelay is the connection monitor delay of the lifecycle test instances
const wsLifecycleMonitorDelay = 100 * time.Millisecond

// wsMockServer is a spot websocket server speaking the MEXC subscription protocol: it echoes an accepted
// subscription's channel in msg, answers a rejected one with the venue's rejection text and answers PING
// with PONG.
type wsMockServer struct {
	*httptest.Server
	upgrader gws.Upgrader

	// refuseUpgrades is the number of upcoming upgrade requests to answer with 503
	refuseUpgrades atomic.Int32
	// refuse, when set, is asked whether to answer an upgrade with 503 given the number of open sockets
	refuse func(open int32) bool
	// pushEvery, when set, sends a PONG frame this often on every connection as background traffic
	pushEvery time.Duration

	attempts atomic.Int32 // upgrade requests received
	open     atomic.Int32 // sockets currently open

	mu      sync.Mutex
	reject  map[string]bool // channels answered with a rejection
	drop    map[string]bool // channels never answered
	silence map[string]bool // channels after whose confirmation the socket stops writing
	subs    map[string]int  // accepted subscriptions per channel
	conns   []*wsMockConn
}

type wsMockConn struct {
	c      *gws.Conn
	wmu    sync.Mutex
	silent atomic.Bool
	closed atomic.Bool
}

func (c *wsMockConn) write(msg string) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = c.c.WriteMessage(gws.TextMessage, []byte(msg))
}

func newWsMockServer(t *testing.T) *wsMockServer {
	t.Helper()
	s := &wsMockServer{
		reject:  map[string]bool{},
		drop:    map[string]bool{},
		silence: map[string]bool{},
		subs:    map[string]int{},
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(func() {
		s.closeAll()
		s.Close()
	})
	return s
}

func (s *wsMockServer) wsURL() string { return "ws" + strings.TrimPrefix(s.URL, "http") }

func (s *wsMockServer) serve(w http.ResponseWriter, r *http.Request) {
	s.attempts.Add(1)
	if s.refuseUpgrades.Load() > 0 {
		s.refuseUpgrades.Add(-1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.refuse != nil && s.refuse(s.open.Load()) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	c, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.open.Add(1)
	mc := &wsMockConn{c: c}
	s.mu.Lock()
	s.conns = append(s.conns, mc)
	s.mu.Unlock()
	stop := make(chan struct{})
	defer func() {
		close(stop)
		mc.closed.Store(true)
		_ = c.Close()
		s.open.Add(-1)
	}()
	if s.pushEvery > 0 {
		go func() {
			tick := time.NewTicker(s.pushEvery)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					if !mc.silent.Load() {
						mc.write(`{"id":0,"code":0,"msg":"PONG"}`)
					}
				}
			}
		}()
	}
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return
		}
		if mc.silent.Load() {
			continue // a stalled peer: the socket stays open but nothing is answered
		}
		var req struct {
			ID     int64    `json:"id"`
			Method string   `json:"method"`
			Params []string `json:"params"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			mc.write(`{"id":0,"code":0,"msg":"msg format invalid"}`)
			continue
		}
		switch strings.ToUpper(req.Method) {
		case "PING":
			mc.write(`{"id":0,"code":0,"msg":"PONG"}`)
		case "SUBSCRIPTION", "UNSUBSCRIPTION":
			for _, p := range req.Params {
				s.mu.Lock()
				drop, reject, silence := s.drop[p], s.reject[p], s.silence[p]
				if !drop && !reject && strings.EqualFold(req.Method, "SUBSCRIPTION") {
					s.subs[p]++
				}
				s.mu.Unlock()
				switch {
				case drop:
				case reject:
					mc.write(`{"id":` + itoa(req.ID) + `,"code":0,"msg":"Not Subscribed successfully! [` + p + `].  Reason： Blocked! "}`)
				default:
					mc.write(`{"id":` + itoa(req.ID) + `,"code":0,"msg":"` + p + `"}`)
				}
				if silence {
					mc.silent.Store(true)
				}
			}
		}
	}
}

func itoa(i int64) string {
	b, _ := json.Marshal(i)
	return string(b)
}

// closeAll drops every open socket from the server side, as the venue does at its daily disconnect
func (s *wsMockServer) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.c.Close()
	}
}

// subscribed returns how many times the channel was accepted
func (s *wsMockServer) subscribed(channel string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subs[channel]
}

// newWsLifecycleExchange builds a MEXC instance from the test configuration with its spot websocket pointed
// at wsURL, a short connection monitor delay and the given subscriptions. tune may adjust the exchange
// configuration before Setup.
func newWsLifecycleExchange(t *testing.T, wsURL string, subs subscription.List, tune func(*config.Exchange)) *Exchange {
	t.Helper()
	root, err := testutils.RootPathFromCWD()
	require.NoError(t, err, "RootPathFromCWD must not error")
	cfg := &config.Config{}
	require.NoError(t, cfg.LoadConfig(filepath.Join(root, "testdata", "configtest.json"), true), "LoadConfig must not error")
	ex := new(Exchange)
	ex.SetDefaults()
	exchConf, err := cfg.GetExchangeConfig(ex.GetName())
	require.NoError(t, err, "GetExchangeConfig must not error")
	exchConf.ConnectionMonitorDelay = wsLifecycleMonitorDelay
	if tune != nil {
		tune(exchConf)
	}
	ex.Websocket = sharedtestvalues.NewTestWebsocket()
	require.NoError(t, ex.Setup(exchConf), "Setup must not error")
	ex.Accounts = accounts.MustNewAccounts(ex)
	require.NoError(t, ex.Websocket.SetAllConnectionURLs(wsURL), "SetAllConnectionURLs must not error")
	ex.Features.Subscriptions = subs
	// Keep the relay from filling up with the monitor's connect errors
	stopDrain := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopDrain:
				return
			case <-ex.Websocket.DataHandler.C:
			}
		}
	}()
	t.Cleanup(func() {
		stopWsLifecycleExchange(ex)
		close(stopDrain)
	})
	return ex
}

// stopWsLifecycleExchange disables the websocket, which is what stops the connection monitor, and waits for
// the monitor to shut it down
func stopWsLifecycleExchange(ex *Exchange) {
	_ = ex.Websocket.Disable()
	deadline := time.Now().Add(20 * wsLifecycleMonitorDelay)
	for ex.Websocket.IsConnected() || ex.Websocket.IsConnecting() {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(wsLifecycleMonitorDelay / 4)
	}
	// Past the next monitor cycle the monitor has seen the websocket disabled and exited
	time.Sleep(3 * wsLifecycleMonitorDelay)
	_ = ex.Websocket.Shutdown()
}

// wsLifecycleDepthSubs is one spot orderbook subscription per enabled pair of the test configuration,
// which enables BTC-USDT and ETH-USDT
func wsLifecycleDepthSubs() subscription.List {
	return subscription.List{{Enabled: true, Asset: asset.Spot, Channel: subscription.OrderbookChannel, Levels: 5}}
}

const (
	wsLifecycleBTCDepth = "spot@public.limit.depth.v3.api.pb@BTCUSDT@5"
	wsLifecycleETHDepth = "spot@public.limit.depth.v3.api.pb@ETHUSDT@5"
)

// TestWsInitialConnectFailureIsRetried starts the spot websocket while the venue refuses the upgrade and
// asserts it comes up on its own, once, with its subscriptions, when the venue starts accepting. Before the
// manager started its connection monitor ahead of the first attempt, a failed first Connect was never
// retried and the websocket stayed down until the process was restarted.
func TestWsInitialConnectFailureIsRetried(t *testing.T) {
	t.Parallel()
	srv := newWsMockServer(t)
	srv.refuseUpgrades.Store(2)
	ex := newWsLifecycleExchange(t, srv.wsURL(), wsLifecycleDepthSubs(), nil)
	t.Cleanup(func() { t.Logf("upgrade requests received: %d", srv.attempts.Load()) })

	start := time.Now()
	require.Error(t, ex.Websocket.Connect(context.Background()), "Connect must fail while the venue refuses the upgrade")
	require.Eventually(t, ex.Websocket.IsConnected, 5*time.Second, 10*time.Millisecond, "the websocket must connect on its own once the venue accepts")
	elapsed := time.Since(start)

	got := ex.Websocket.GetSubscriptions()
	require.Len(t, got, 2, "both subscriptions must be restored by the automatic connect")
	for _, s := range got {
		assert.Equalf(t, subscription.SubscribedState, s.State(), "%s should be subscribed", s.QualifiedChannel)
	}
	assert.Equal(t, 1, srv.subscribed(wsLifecycleBTCDepth), "the venue should have accepted the BTC subscription once")
	assert.Equal(t, 1, srv.subscribed(wsLifecycleETHDepth), "the venue should have accepted the ETH subscription once")

	attempts := srv.attempts.Load()
	assert.EqualValues(t, 3, attempts, "two refused upgrades and one accepted one should be all the dialling done")
	assert.LessOrEqual(t, int64(attempts), int64(elapsed/wsLifecycleMonitorDelay)+2, "retries should be paced by the monitor delay")
	time.Sleep(5 * wsLifecycleMonitorDelay)
	assert.Equal(t, attempts, srv.attempts.Load(), "a connected websocket should not dial again")
}

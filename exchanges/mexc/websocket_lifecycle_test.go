package mexc

import (
	"context"
	"errors"
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
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/encoding/json"
	"github.com/thrasher-corp/gocryptotrader/exchange/accounts"
	"github.com/thrasher-corp/gocryptotrader/exchange/websocket"
	exchange "github.com/thrasher-corp/gocryptotrader/exchanges"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/kline"
	"github.com/thrasher-corp/gocryptotrader/exchanges/request"
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

// lossySubscriptionConn confirms every subscription request except those whose channel it is told to
// fail, for which it returns the configured error. It records the channels it was asked for.
type lossySubscriptionConn struct {
	websocket.Connection
	fail      map[string]error
	requested []string
}

func (c *lossySubscriptionConn) SendMessageReturnResponse(_ context.Context, _ request.EndpointLimit, _, req any) ([]byte, error) {
	p, ok := req.(*WsSubscriptionPayload)
	if !ok || len(p.Params) != 1 {
		return nil, errors.New("unexpected subscription payload")
	}
	c.requested = append(c.requested, p.Params[0])
	if err := c.fail[p.Params[0]]; err != nil {
		return nil, err
	}
	return []byte(`{"id":` + itoa(p.ID) + `,"code":0,"msg":"` + p.Params[0] + `"}`), nil
}

func lossyTestSubs() subscription.List {
	subs := make(subscription.List, 4)
	for i, base := range []currency.Code{currency.BTC, currency.ETH, currency.SOL, currency.XRP} {
		p := currency.NewPair(base, currency.USDT)
		subs[i] = &subscription.Subscription{Channel: channelLimitDepthV3, Asset: asset.Spot, Pairs: currency.Pairs{p}, Levels: 5, QualifiedChannel: "spot@public.limit.depth.v3.api.pb@" + base.String() + "USDT@5"}
	}
	return subs
}

func registeredChannels(ex *Exchange) []string {
	var got []string
	for _, s := range ex.Websocket.GetSubscriptions() {
		got = append(got, s.QualifiedChannel)
	}
	return got
}

// TestHandleSubscriptionOutlivesALostConfirmation keeps what the venue confirmed when a later confirmation
// is lost, and still requests the channels after it. Returning on the first lost confirmation left every
// channel confirmed before it live but unregistered, and never requested the ones after it, while the
// connection stayed up: those channels were missing until the next reconnect, which on a busy connection
// is the venue's daily disconnect.
func TestHandleSubscriptionOutlivesALostConfirmation(t *testing.T) {
	t.Parallel()
	subs := lossyTestSubs()
	ex := newSignedTestExchange(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	conn := &lossySubscriptionConn{fail: map[string]error{subs[1].QualifiedChannel: websocket.ErrSignatureTimeout}}

	err := ex.handleSubscription(t.Context(), conn, "SUBSCRIPTION", subs)
	require.ErrorIs(t, err, websocket.ErrSubscriptionFailure, "the unconfirmed channel must be reported")
	require.ErrorIs(t, err, websocket.ErrSignatureTimeout, "the cause must be kept")
	assert.ErrorContains(t, err, "ETH/USDT", "the error should name the unconfirmed channel")
	assert.Len(t, conn.requested, 4, "the channels after the lost confirmation should still be requested")
	assert.ElementsMatch(t, []string{subs[0].QualifiedChannel, subs[2].QualifiedChannel, subs[3].QualifiedChannel}, registeredChannels(ex), "every confirmed channel should be registered")
}

// TestHandleSubscriptionStopsOnADeadConnection stops requesting once the connection is shown to be gone,
// by a second confirmation lost in a row or by a failed send, and still registers what was confirmed.
func TestHandleSubscriptionStopsOnADeadConnection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		fail func(subs subscription.List) map[string]error
		sent int
	}{
		{"two lost in a row", func(subs subscription.List) map[string]error {
			return map[string]error{subs[1].QualifiedChannel: websocket.ErrSignatureTimeout, subs[2].QualifiedChannel: websocket.ErrSignatureTimeout}
		}, 3},
		{"send failure", func(subs subscription.List) map[string]error {
			return map[string]error{subs[1].QualifiedChannel: websocket.ErrNotConnected}
		}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subs := lossyTestSubs()
			ex := newSignedTestExchange(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			conn := &lossySubscriptionConn{fail: tc.fail(subs)}
			err := ex.handleSubscription(t.Context(), conn, "SUBSCRIPTION", subs)
			require.ErrorIs(t, err, websocket.ErrSubscriptionFailure, "the failure must be reported")
			assert.Len(t, conn.requested, tc.sent, "nothing should be requested once the connection is gone")
			assert.Equal(t, []string{subs[0].QualifiedChannel}, registeredChannels(ex), "the channel confirmed before the failure should be registered")
			assert.ErrorContains(t, err, "XRP/USDT", "the channels never requested should be named in the error")
		})
	}
}

// TestWsConnectOutlivesALostConfirmation drives the same loss through Connect against a venue that never
// answers one subscription: the websocket must come up with every other channel registered.
func TestWsConnectOutlivesALostConfirmation(t *testing.T) {
	t.Parallel()
	srv := newWsMockServer(t)
	srv.drop[wsLifecycleETHDepth] = true
	subs := subscription.List{
		{Enabled: true, Asset: asset.Spot, Channel: subscription.OrderbookChannel, Levels: 5},
		{Enabled: true, Asset: asset.Spot, Channel: subscription.AllTradesChannel, Interval: kline.HundredMilliseconds},
	}
	ex := newWsLifecycleExchange(t, srv.wsURL(), subs, nil)

	err := ex.Websocket.Connect(context.Background())
	require.ErrorIs(t, err, websocket.ErrSubscriptionFailure, "the unanswered subscription must be reported")
	require.True(t, ex.Websocket.IsConnected(), "the connection must stay up")
	assert.ElementsMatch(t, []string{
		wsLifecycleBTCDepth,
		"spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT",
		"spot@public.aggre.deals.v3.api.pb@100ms@ETHUSDT",
	}, registeredChannels(ex), "every channel the venue confirmed should be registered")
	assert.Equal(t, 1, srv.subscribed("spot@public.aggre.deals.v3.api.pb@100ms@ETHUSDT"), "the channels after the unanswered one should be requested")
}

// listenKeyLedger is a REST server minting and closing listen keys
type listenKeyLedger struct {
	mu                      sync.Mutex
	minted, closed, renewed int
	live                    map[string]bool
	client                  *http.Client
}

func (l *listenKeyLedger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch r.Method {
	case http.MethodPost:
		l.minted++
		k := "KEY" + itoa(int64(l.minted))
		l.live[k] = true
		_, _ = w.Write([]byte(`{"listenKey":"` + k + `"}`))
		return
	case http.MethodDelete:
		l.closed++
		delete(l.live, r.URL.Query().Get("listenKey"))
	case http.MethodPut:
		l.renewed++
	}
	_, _ = w.Write([]byte(`{}`))
}

func (l *listenKeyLedger) counts() (minted, closed, live int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.minted, l.closed, len(l.live)
}

// withListenKeyREST points the exchange's REST endpoints at a listen key ledger and enables the
// authenticated websocket
func withListenKeyREST(t *testing.T, ex *Exchange) *listenKeyLedger {
	t.Helper()
	l := &listenKeyLedger{live: map[string]bool{}}
	srv := httptest.NewServer(l)
	t.Cleanup(srv.Close)
	ex.SetCredentials(&accounts.Credentials{Key: testCredentialKey, Secret: testCredentialSecret})
	ex.GetBase().SkipAuthCheck = true
	require.NoError(t, ex.Requester.DisableRateLimiter(), "DisableRateLimiter must not error")
	l.client = srv.Client()
	require.NoError(t, ex.SetHTTPClient(l.client), "SetHTTPClient must not error")
	require.NoError(t, ex.API.Endpoints.SetRunningURL(exchange.RestSpot.String(), srv.URL), "SetRunningURL must not error")
	ex.Websocket.SetCanUseAuthenticatedEndpoints(true)
	return l
}

// TestKeepListenKeyAliveReleasesPromptlyWhenItsConnectionCloses releases a listen key within a few seconds
// of its connection closing, not at the next renewal half an hour later.
func TestKeepListenKeyAliveReleasesPromptlyWhenItsConnectionCloses(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var closed []string
	ex := newSignedTestExchange(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			closed = append(closed, r.URL.Query().Get("listenKey"))
			mu.Unlock()
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	conn := &listenKeyTestConn{}
	done := make(chan struct{})
	go func() {
		ex.keepListenKeyAlive(context.Background(), conn, "KEY_A")
		close(done)
	}()
	conn.closed.Store(true)
	select {
	case <-done:
	case <-time.After(3 * listenKeyConnCheckInterval):
		require.Fail(t, "the renewer must stop soon after its connection closes")
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"KEY_A"}, closed, "the renewer should close its listen key")
}

// TestWsRollbackDoesNotHoldListenKeys keeps a second connection of an authenticated websocket failing, so
// every monitor cycle opens the first connection with a fresh listen key and then rolls it back. Once the
// venue accepts both, the account must hold only the two keys in use. Renewers that noticed a rolled-back
// connection only at their next renewal kept one key per failed cycle for half an hour, and the venue caps
// the keys an account may hold.
func TestWsRollbackDoesNotHoldListenKeys(t *testing.T) {
	t.Parallel()
	srv := newWsMockServer(t)
	var refusing atomic.Bool
	refusing.Store(true)
	srv.refuse = func(open int32) bool { return refusing.Load() && open >= 1 }
	ex := newWsLifecycleExchange(t, srv.wsURL(), wsLifecycleDepthSubs(), nil)
	ex.Websocket.MaxSubscriptionsPerConnection = 1
	ledger := withListenKeyREST(t, ex)

	require.Error(t, ex.Websocket.Connect(context.Background()), "Connect must fail while the second connection is refused")
	require.Eventually(t, func() bool { minted, _, _ := ledger.counts(); return minted >= 8 }, 5*time.Second, 10*time.Millisecond, "the monitor must keep retrying")
	refusing.Store(false)
	require.Eventually(t, ex.Websocket.IsConnected, 5*time.Second, 10*time.Millisecond, "the websocket must connect once both connections are accepted")
	require.EqualValues(t, 2, srv.open.Load(), "both connections must be open")
	assert.Eventually(t, func() bool { _, _, live := ledger.counts(); return live == 2 }, 3*listenKeyConnCheckInterval, 10*time.Millisecond, "only the two keys in use should be held")
}

// TestWsReadIdleTimeoutSparesAQuietConnection keeps the idle bound clear of the PING cycle: a healthy
// connection subscribed to quiet channels still receives a PONG every wsPingInterval.
func TestWsReadIdleTimeoutSparesAQuietConnection(t *testing.T) {
	t.Parallel()
	assert.GreaterOrEqual(t, wsReadIdleTimeout, 2*wsPingInterval, "the idle bound should outlast at least two unanswered PINGs")
}

// TestWsSilentConnectionIsReplaced spreads two subscriptions over two connections and lets one of them go
// silent while the other keeps receiving. The silent connection must be reported lost and the websocket
// reconnected. The manager's traffic monitor watches all connections together, so without a bound on
// each connection's reads the silent one went unnoticed while the other received.
// It shortens wsReadIdleTimeout, so it does not run in parallel.
func TestWsSilentConnectionIsReplaced(t *testing.T) {
	prev := wsReadIdleTimeout
	wsReadIdleTimeout = 300 * time.Millisecond
	t.Cleanup(func() { wsReadIdleTimeout = prev })

	for _, tc := range []struct {
		name   string
		silent bool
	}{
		{"one connection silent", true},
		{"both connections receiving", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newWsMockServer(t)
			srv.pushEvery = 50 * time.Millisecond
			srv.silence[wsLifecycleETHDepth] = tc.silent
			ex := newWsLifecycleExchange(t, srv.wsURL(), wsLifecycleDepthSubs(), nil)
			ex.Websocket.MaxSubscriptionsPerConnection = 1
			require.NoError(t, ex.Websocket.Connect(context.Background()), "Connect must not error")
			require.EqualValues(t, 2, srv.attempts.Load(), "each subscription must get its own connection")
			if tc.silent {
				assert.Eventually(t, func() bool { return srv.attempts.Load() > 2 }, 3*time.Second, 10*time.Millisecond, "the silent connection should be dropped and the websocket reconnected")
				return
			}
			time.Sleep(10 * wsReadIdleTimeout)
			assert.EqualValues(t, 2, srv.attempts.Load(), "connections which keep receiving should not be dropped")
			assert.True(t, ex.Websocket.IsConnected(), "the websocket should stay connected")
		})
	}
}

package engine

import (
	"testing"

	gws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrasher-corp/gocryptotrader/exchanges/binance"
	"github.com/thrasher-corp/gocryptotrader/gctrpc"
	testexch "github.com/thrasher-corp/gocryptotrader/internal/testing/exchange"
	mockws "github.com/thrasher-corp/gocryptotrader/internal/testing/websocket"
)

// TestWebsocketGetInfoReportsConnection pins AD-324: WebsocketGetInfo says whether the websocket is
// connected right now, which enabled does not, and always sets the field, so its absence identifies a
// server predating it
func TestWebsocketGetInfoReportsConnection(t *testing.T) {
	t.Parallel()
	e := testexch.MockWsInstance[binance.Exchange](t, mockws.CurryWsMockUpgrader(t, func(testing.TB, []byte, *gws.Conn) error { return nil }))
	em := NewExchangeManager()
	require.NoError(t, em.Add(e), "adding the exchange must not error")
	s := RPCServer{Engine: &Engine{ExchangeManager: em}}

	resp, err := s.WebsocketGetInfo(t.Context(), &gctrpc.WebsocketGetInfoRequest{Exchange: e.GetName()})
	require.NoError(t, err, "WebsocketGetInfo must not error")
	require.NotNil(t, resp.Connected, "connected must always be set")
	assert.True(t, resp.GetConnected(), "a connected websocket should be reported connected")
	assert.True(t, resp.Enabled, "the websocket should be enabled")

	require.NoError(t, e.Websocket.Shutdown(), "Shutdown must not error")
	resp, err = s.WebsocketGetInfo(t.Context(), &gctrpc.WebsocketGetInfoRequest{Exchange: e.GetName()})
	require.NoError(t, err, "WebsocketGetInfo must not error")
	require.NotNil(t, resp.Connected, "connected must be set when the websocket is down too")
	assert.False(t, resp.GetConnected(), "a websocket that is down should be reported disconnected")
	assert.True(t, resp.Enabled, "a websocket that is down can still be enabled, which is why connected is needed")
}

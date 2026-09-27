package mexc

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchange/stream"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/mexc/mexc_proto_types"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
)

// restRecentTradesWithoutIDs is the venue's REST recent trades answer: it carries no trade id (id is null)
const restRecentTradesWithoutIDs = `[{"id":null,"price":"77234.92","qty":"0.1","quoteQty":"7723.492","time":1789251246575,"isBuyerMaker":true,"isBestMatch":true,"tradeType":"ASK"}]`

// newRecentTradesTestExchange returns an exchange whose REST recent trades endpoint answers without trade
// ids and counts its calls, with the websocket data handler ready to take pushes
func newRecentTradesTestExchange(t *testing.T) (*Exchange, *atomic.Int32) {
	t.Helper()
	var restCalls atomic.Int32
	ex := newSignedTestExchange(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/trades") {
			restCalls.Add(1)
		}
		_, _ = w.Write([]byte(restRecentTradesWithoutIDs))
	}))
	ex.Websocket.DataHandler = stream.NewRelay(10)
	return ex, &restCalls
}

// TestGetRecentTradesAnswersFromWebsocketWithTradeIDs pins INC-323 option B: a public websocket trade,
// the only source carrying the venue's trade id, is what GetRecentTrades returns, with its id and its
// millisecond time as sent, and the REST endpoint is not asked once the stream has delivered trades.
func TestGetRecentTradesAnswersFromWebsocketWithTradeIDs(t *testing.T) {
	t.Parallel()
	ex, restCalls := newRecentTradesTestExchange(t)
	ex.SetSaveTradeDataStatus(false)
	ex.SetTradeFeedStatus(false)
	frame := wsPushFrame(t, "spot@"+channelAggreDealsV3+"@100ms@"+wsTestSymbol, 1736409765052,
		&mexc_proto_types.PublicAggreDealsV3Api{Deals: []*mexc_proto_types.PublicAggreDealsV3ApiItem{
			{Price: "93220.00", Quantity: "0.04438243", TradeType: 1, Time: 1736409765051, TradeId: "731579883561406466X0_731579883561406467X0"},
			{Price: "93221.50", Quantity: "1.5", TradeType: 2, Time: 1736409765099, TradeId: "731579883561406468X1_731579883561406469X1"},
		}})
	require.NoError(t, ex.WsHandleData(t.Context(), nil, frame), "WsHandleData must not error")
	assert.Empty(t, ex.Websocket.DataHandler.C, "nothing should be relayed with the trade feed off")

	got, err := ex.GetRecentTrades(t.Context(), currency.NewBTCUSDT(), asset.Spot)
	require.NoError(t, err, "GetRecentTrades must not error")
	require.Len(t, got, 2, "GetRecentTrades must return both websocket trades")
	assert.Equal(t, "731579883561406466X0_731579883561406467X0", got[0].TID, "the first trade should carry its venue trade id")
	assert.Equal(t, "731579883561406468X1_731579883561406469X1", got[1].TID, "the second trade should carry its venue trade id")
	assert.Equal(t, int64(1736409765051), got[0].Timestamp.UnixMilli(), "the trade time should keep its milliseconds")
	assert.Equal(t, int64(1736409765099), got[1].Timestamp.UnixMilli(), "the trade time should keep its milliseconds")
	assert.Equal(t, order.Buy, got[0].Side, "tradeType 1 should be a buy")
	assert.Equal(t, order.Sell, got[1].Side, "any other tradeType should be a sell")
	assert.Equal(t, 93220.00, got[0].Price, "the price should be as sent")
	assert.Equal(t, 0.04438243, got[0].Amount, "the amount should be as sent")
	assert.Zero(t, restCalls.Load(), "the REST endpoint should not be asked once the stream has trades")
}

// TestGetRecentTradesFallsBackToRESTWithoutTradeIDs pins the fallback: with no websocket trades for the
// pair the REST endpoint answers as before, and its trades carry no id because the venue sends none;
// none is made up.
func TestGetRecentTradesFallsBackToRESTWithoutTradeIDs(t *testing.T) {
	t.Parallel()
	ex, restCalls := newRecentTradesTestExchange(t)
	got, err := ex.GetRecentTrades(t.Context(), currency.NewBTCUSDT(), asset.Spot)
	require.NoError(t, err, "GetRecentTrades must not error")
	require.Len(t, got, 1, "GetRecentTrades must return the REST trade")
	assert.Empty(t, got[0].TID, "a REST trade should carry no trade id, since the venue sends none")
	assert.Equal(t, int64(1789251246575), got[0].Timestamp.UnixMilli(), "the REST trade time should be as sent")
	assert.Equal(t, int32(1), restCalls.Load(), "the REST endpoint should answer when the stream has no trades")
}

// TestGetRecentTradesStreamIsPerPair pins that websocket trades for one pair do not answer for another:
// that pair still goes to REST
func TestGetRecentTradesStreamIsPerPair(t *testing.T) {
	t.Parallel()
	ex, restCalls := newRecentTradesTestExchange(t)
	frame := wsPushFrame(t, "spot@"+channelAggreDealsV3+"@100ms@"+wsTestSymbol, 1736409765052,
		&mexc_proto_types.PublicAggreDealsV3Api{Deals: []*mexc_proto_types.PublicAggreDealsV3ApiItem{
			{Price: "1", Quantity: "1", TradeType: 1, Time: 1736409765051, TradeId: "t-1"},
		}})
	require.NoError(t, ex.WsHandleData(t.Context(), nil, frame), "WsHandleData must not error")
	got, err := ex.GetRecentTrades(t.Context(), currency.NewPair(currency.ETH, currency.USDT), asset.Spot)
	require.NoError(t, err, "GetRecentTrades must not error")
	require.Len(t, got, 1, "the other pair must get the REST answer")
	assert.Empty(t, got[0].TID, "the other pair should not receive the streamed trade")
	assert.Equal(t, int32(1), restCalls.Load(), "the other pair should be answered by REST")
}

// TestRecentTradesBufferIsBounded pins that the websocket trades kept per pair never exceed the bound,
// the newest kept, however many arrive
func TestRecentTradesBufferIsBounded(t *testing.T) {
	t.Parallel()
	ex, _ := newRecentTradesTestExchange(t)
	deals := make([]*mexc_proto_types.PublicAggreDealsV3ApiItem, 0, recentTradesPerPair+5)
	for i := range int64(recentTradesPerPair + 5) {
		deals = append(deals, &mexc_proto_types.PublicAggreDealsV3ApiItem{Price: "1", Quantity: "1", TradeType: 1, Time: 1736409765000 + i, TradeId: "t"})
	}
	frame := wsPushFrame(t, "spot@"+channelAggreDealsV3+"@100ms@"+wsTestSymbol, 1736409766052,
		&mexc_proto_types.PublicAggreDealsV3Api{Deals: deals})
	require.NoError(t, ex.WsHandleData(t.Context(), nil, frame), "WsHandleData must not error")
	got, err := ex.GetRecentTrades(t.Context(), currency.NewBTCUSDT(), asset.Spot)
	require.NoError(t, err, "GetRecentTrades must not error")
	require.Len(t, got, recentTradesPerPair, "no more than the bound should be kept")
	assert.Equal(t, int64(1736409765005), got[0].Timestamp.UnixMilli(), "the oldest trades should be the ones dropped")
	assert.Equal(t, int64(1736409765000+recentTradesPerPair+4), got[len(got)-1].Timestamp.UnixMilli(), "the newest trade should be kept")
}

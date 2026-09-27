package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrasher-corp/gocryptotrader/currency"
	exchange "github.com/thrasher-corp/gocryptotrader/exchanges"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
	"github.com/thrasher-corp/gocryptotrader/exchanges/trade"
	"github.com/thrasher-corp/gocryptotrader/gctrpc"
)

type recentTradesExchange struct {
	exchange.IBotExchange
	trades []trade.Data
}

func (r recentTradesExchange) GetRecentTrades(context.Context, currency.Pair, asset.Item) ([]trade.Data, error) {
	return r.trades, nil
}

// TestGetRecentTradesCarriesMilliseconds pins that a trade's time reaches the client to the millisecond
// in timestamp_ms, while timestamp keeps its format to the second, and that its trade id is carried
func TestGetRecentTradesCarriesMilliseconds(t *testing.T) {
	t.Parallel()
	em := NewExchangeManager()
	exch, err := em.NewExchangeByName(testExchange)
	require.NoError(t, err, "NewExchangeByName must not error")
	b := exch.GetBase()
	b.Name = "recenttrades"
	b.Enabled = true
	cp := currency.NewBTCUSDT()
	b.CurrencyPairs.Pairs = map[asset.Item]*currency.PairStore{asset.Spot: {
		Available:     currency.Pairs{cp},
		Enabled:       currency.Pairs{cp},
		AssetEnabled:  true,
		ConfigFormat:  &currency.PairFormat{Uppercase: true},
		RequestFormat: &currency.PairFormat{Uppercase: true},
	}}
	at := time.UnixMilli(1736409765051)
	require.NoError(t, em.Add(recentTradesExchange{IBotExchange: exch, trades: []trade.Data{{
		TID: "731579883561406466X0_731579883561406467X0", Exchange: "recenttrades", CurrencyPair: cp, AssetType: asset.Spot,
		Side: order.Buy, Price: 93220, Amount: 0.04438243, Timestamp: at,
	}}}), "adding the exchange must not error")
	s := RPCServer{Engine: &Engine{ExchangeManager: em}}

	resp, err := s.GetRecentTrades(t.Context(), &gctrpc.GetSavedTradesRequest{
		Exchange:  "recenttrades",
		Pair:      &gctrpc.CurrencyPair{Delimiter: currency.DashDelimiter, Base: cp.Base.String(), Quote: cp.Quote.String()},
		AssetType: asset.Spot.String(),
	})
	require.NoError(t, err, "GetRecentTrades must not error")
	require.Len(t, resp.Trades, 1, "the trade must be returned")
	assert.Equal(t, int64(1736409765051), resp.Trades[0].TimestampMs, "timestamp_ms should keep the milliseconds")
	assert.Equal(t, "2025-01-09 08:02:45 UTC", resp.Trades[0].Timestamp, "timestamp should keep its format to the second")
	assert.Equal(t, "731579883561406466X0_731579883561406467X0", resp.Trades[0].TradeId, "the trade id should be carried")
}

package trade

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	"github.com/thrasher-corp/gocryptotrader/exchanges/order"
)

func recentTrade(exch string, p currency.Pair, tid string, ms int64) Data {
	return Data{
		TID:          tid,
		Exchange:     exch,
		CurrencyPair: p,
		AssetType:    asset.Spot,
		Side:         order.Buy,
		Price:        1,
		Amount:       1,
		Timestamp:    time.UnixMilli(ms),
	}
}

func TestNewRecentBuffer(t *testing.T) {
	t.Parallel()
	_, err := NewRecentBuffer(0)
	require.ErrorIs(t, err, ErrInvalidRecentCapacity, "a zero capacity must be refused")
	_, err = NewRecentBuffer(-1)
	require.ErrorIs(t, err, ErrInvalidRecentCapacity, "a negative capacity must be refused")
	b, err := NewRecentBuffer(1)
	require.NoError(t, err, "NewRecentBuffer must not error")
	assert.Nil(t, b.Get("x", asset.Spot, currency.NewBTCUSDT()), "an empty buffer should have no trades")
}

// TestRecentBufferKeepsTradesAsGiven pins that a trade comes back exactly as stored: its trade id and
// its millisecond timestamp survive, nothing is truncated to the second
func TestRecentBufferKeepsTradesAsGiven(t *testing.T) {
	t.Parallel()
	b, err := NewRecentBuffer(10)
	require.NoError(t, err, "NewRecentBuffer must not error")
	btc := currency.NewBTCUSDT()
	in := recentTrade("MEXC", btc, "731579883561406466X0_731579883561406467X0", 1736409765051)
	require.NoError(t, b.Add(in), "Add must not error")
	got := b.Get("MEXC", asset.Spot, btc)
	require.Len(t, got, 1, "the stored trade must be returned")
	assert.Equal(t, in, got[0], "the trade should come back unchanged")
	assert.Equal(t, int64(1736409765051), got[0].Timestamp.UnixMilli(), "the millisecond timestamp should be kept")
}

func TestRecentBufferEvictsOldestPerPair(t *testing.T) {
	t.Parallel()
	b, err := NewRecentBuffer(3)
	require.NoError(t, err, "NewRecentBuffer must not error")
	btc, eth := currency.NewBTCUSDT(), currency.NewPair(currency.ETH, currency.USDT)
	for i := range int64(5) {
		require.NoError(t, b.Add(recentTrade("MEXC", btc, "b", 1000+i)), "Add must not error")
	}
	require.NoError(t, b.Add(recentTrade("MEXC", eth, "e", 1)), "Add must not error")
	got := b.Get("MEXC", asset.Spot, btc)
	require.Len(t, got, 3, "a pair must hold no more than its capacity")
	for i, want := range []int64{1002, 1003, 1004} {
		assert.Equalf(t, want, got[i].Timestamp.UnixMilli(), "trade %d should be the newest kept, oldest first", i)
	}
	assert.Len(t, b.Get("MEXC", asset.Spot, eth), 1, "another pair should keep its own trades")
	assert.Nil(t, b.Get("OTHER", asset.Spot, btc), "another exchange should see none of them")
	assert.Nil(t, b.Get("MEXC", asset.Futures, btc), "another asset should see none of them")
}

func TestRecentBufferMatchesPairWhateverDelimiter(t *testing.T) {
	t.Parallel()
	b, err := NewRecentBuffer(2)
	require.NoError(t, err, "NewRecentBuffer must not error")
	require.NoError(t, b.Add(recentTrade("MEXC", currency.NewPairWithDelimiter("BTC", "USDT", ""), "t", 1)), "Add must not error")
	assert.Len(t, b.Get("MEXC", asset.Spot, currency.NewPairWithDelimiter("BTC", "USDT", "-")), 1, "the pair should match on its currencies")
}

func TestRecentBufferRejectsUnkeyedTrades(t *testing.T) {
	t.Parallel()
	b, err := NewRecentBuffer(2)
	require.NoError(t, err, "NewRecentBuffer must not error")
	btc := currency.NewBTCUSDT()
	good := recentTrade("MEXC", btc, "t", 1)
	noExchange := recentTrade("", btc, "t", 2)
	require.ErrorIs(t, b.Add(good, noExchange), errRecentTradeUnkeyed, "a trade without an exchange must be refused")
	noPair := recentTrade("MEXC", currency.EMPTYPAIR, "t", 3)
	require.ErrorIs(t, b.Add(noPair), errRecentTradeUnkeyed, "a trade without a pair must be refused")
	noAsset := recentTrade("MEXC", btc, "t", 4)
	noAsset.AssetType = asset.Empty
	require.ErrorIs(t, b.Add(noAsset), errRecentTradeUnkeyed, "a trade without an asset must be refused")
	assert.Nil(t, b.Get("MEXC", asset.Spot, btc), "a refused batch should store nothing")
}

func TestRecentBufferGetReturnsACopy(t *testing.T) {
	t.Parallel()
	b, err := NewRecentBuffer(2)
	require.NoError(t, err, "NewRecentBuffer must not error")
	btc := currency.NewBTCUSDT()
	require.NoError(t, b.Add(recentTrade("MEXC", btc, "t", 1)), "Add must not error")
	got := b.Get("MEXC", asset.Spot, btc)
	got[0].TID = "changed"
	assert.Equal(t, "t", b.Get("MEXC", asset.Spot, btc)[0].TID, "changing a returned trade should not change the buffer")
}

func TestRecentBufferConcurrentUse(t *testing.T) {
	t.Parallel()
	b, err := NewRecentBuffer(50)
	require.NoError(t, err, "NewRecentBuffer must not error")
	btc := currency.NewBTCUSDT()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 100 {
				assert.NoError(t, b.Add(recentTrade("MEXC", btc, "t", int64(w*1000+i))), "Add should not error")
				assert.LessOrEqual(t, len(b.Get("MEXC", asset.Spot, btc)), 50, "a pair should never exceed its capacity")
			}
		})
	}
	wg.Wait()
	assert.Len(t, b.Get("MEXC", asset.Spot, btc), 50, "the pair should end full")
}

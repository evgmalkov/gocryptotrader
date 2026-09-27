package trade

import (
	"errors"
	"fmt"
	"sync"

	"github.com/thrasher-corp/gocryptotrader/common/key"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
)

// ErrInvalidRecentCapacity is returned when a recent trades buffer is asked to hold fewer than one trade per pair
var ErrInvalidRecentCapacity = errors.New("recent trades capacity must be at least one")

var errRecentTradeUnkeyed = errors.New("recent trade has no exchange, asset or pair")

// RecentBuffer keeps, for each exchange, asset and pair, the latest trades a stream delivered, in the
// order they arrived. Each pair holds at most capacity trades and drops its oldest first, so memory is
// bounded by capacity times the pairs streamed however long the stream runs. It lets an exchange answer
// recent trades from its stream when the stream carries what its REST endpoint omits, such as trade ids.
// It lives only in memory and needs no database.
type RecentBuffer struct {
	capacity int
	mu       sync.RWMutex
	pairs    map[key.ExchangeAssetPair]*recentRing
}

// recentRing is one pair's trades: it grows by append up to capacity, then overwrites its oldest trade
type recentRing struct {
	trades []Data
	oldest int
}

// NewRecentBuffer returns an empty buffer holding up to capacity trades per exchange, asset and pair
func NewRecentBuffer(capacity int) (*RecentBuffer, error) {
	if capacity < 1 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidRecentCapacity, capacity)
	}
	return &RecentBuffer{capacity: capacity, pairs: make(map[key.ExchangeAssetPair]*recentRing)}, nil
}

// Add stores trades as the most recent for their exchange, asset and pair, evicting the oldest once a
// pair is full. Trades are stored by value, exactly as given: nothing is truncated, rounded or filled in.
// A trade missing its exchange, asset or pair is rejected, and none of the trades are stored.
func (b *RecentBuffer) Add(trades ...Data) error {
	for i := range trades {
		if trades[i].Exchange == "" || !trades[i].AssetType.IsValid() || trades[i].CurrencyPair.IsEmpty() {
			return fmt.Errorf("%w: %+v", errRecentTradeUnkeyed, trades[i])
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range trades {
		k := key.NewExchangeAssetPair(trades[i].Exchange, trades[i].AssetType, trades[i].CurrencyPair)
		r, ok := b.pairs[k]
		if !ok {
			r = &recentRing{}
			b.pairs[k] = r
		}
		if len(r.trades) < b.capacity {
			r.trades = append(r.trades, trades[i])
			continue
		}
		r.trades[r.oldest] = trades[i]
		r.oldest = (r.oldest + 1) % b.capacity
	}
	return nil
}

// Get returns a copy of the trades held for an exchange, asset and pair, oldest first, or nil when none
// have arrived. The pair matches on its currencies, whatever its delimiter.
func (b *RecentBuffer) Get(exchangeName string, a asset.Item, p currency.Pair) []Data {
	b.mu.RLock()
	defer b.mu.RUnlock()
	r, ok := b.pairs[key.NewExchangeAssetPair(exchangeName, a, p)]
	if !ok || len(r.trades) == 0 {
		return nil
	}
	out := make([]Data, 0, len(r.trades))
	out = append(out, r.trades[r.oldest:]...)
	return append(out, r.trades[:r.oldest]...)
}

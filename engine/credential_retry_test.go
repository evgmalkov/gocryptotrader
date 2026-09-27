package engine

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	exchange "github.com/thrasher-corp/gocryptotrader/exchanges"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
)

var errCredentialLookup = &url.Error{Op: "Get", URL: "https://api.example.com/api/v3/account", Err: &net.DNSError{Err: "server misbehaving", Name: "api.example.com"}}

// TestValidateAPICredentialsRetriesAfterNetworkError pins AD-323: credentials that could not be checked
// because the venue was unreachable are checked again, so a network blip at start does not disable
// authenticated support for good
func TestValidateAPICredentialsRetriesAfterNetworkError(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		start := time.Now()
		err := validateAPICredentialsWithNetworkRetry(t.Context(), testExchange, asset.Items{asset.Spot}, func(context.Context, asset.Item) error {
			if calls.Add(1) == 1 {
				return errCredentialLookup
			}
			return nil
		}, exchange.NetworkRetryDelays)
		require.NoError(t, err, "validation must succeed once the venue answers")
		assert.Equal(t, int32(2), calls.Load(), "validation should run again after the network failure, then stop")
		assert.Equal(t, exchange.NetworkRetryDelays[0], time.Since(start), "the retry should wait its delay")
	})
}

func TestValidateAPICredentialsGivesUpAfterRetries(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		err := validateAPICredentialsWithNetworkRetry(t.Context(), testExchange, asset.Items{asset.Spot}, func(context.Context, asset.Item) error {
			calls.Add(1)
			return errCredentialLookup
		}, exchange.NetworkRetryDelays)
		require.ErrorIs(t, err, errCredentialLookup, "validation must fail once the retries run out")
		assert.Equal(t, int32(len(exchange.NetworkRetryDelays)+1), calls.Load(), "validation should make five attempts in all")
	})
}

// TestValidateAPICredentialsDoesNotRetryRejectedCredentials pins that only a network failure is retried:
// credentials the venue rejected are reported straight away
func TestValidateAPICredentialsDoesNotRetryRejectedCredentials(t *testing.T) {
	t.Parallel()
	errRejected := errors.New("invalid api key")
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		start := time.Now()
		err := validateAPICredentialsWithNetworkRetry(t.Context(), testExchange, asset.Items{asset.Spot}, func(context.Context, asset.Item) error {
			calls.Add(1)
			return errRejected
		}, exchange.NetworkRetryDelays)
		require.ErrorIs(t, err, errRejected, "rejected credentials must be reported")
		assert.Equal(t, int32(1), calls.Load(), "rejected credentials should not be checked again")
		assert.Zero(t, time.Since(start), "rejected credentials should be reported without waiting")
	})
}

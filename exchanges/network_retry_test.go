package exchange_test

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
	shared "github.com/thrasher-corp/gocryptotrader/exchanges/sharedtestvalues"
)

// errLookup is the failure seen when the venue's host cannot be resolved at start
var errLookup = &url.Error{Op: "Get", URL: "https://api.example.com/api/v3/exchangeInfo", Err: &net.DNSError{Err: "server misbehaving", Name: "api.example.com"}}

var errVenueAnswer = errors.New("venue refused the request")

func TestIsNetworkError(t *testing.T) {
	t.Parallel()
	assert.True(t, exchange.IsNetworkError(errLookup), "a failed lookup should be a network error")
	assert.True(t, exchange.IsNetworkError(errors.Join(errVenueAnswer, errLookup)), "a joined network error should be found")
	assert.False(t, exchange.IsNetworkError(errVenueAnswer), "an answer from the venue should not be a network error")
	assert.False(t, exchange.IsNetworkError(nil), "no error should not be a network error")
}

// TestRetryAfterNetworkErrorRecovers pins AD-323: a step that failed on the network at start is tried
// again after the first delay and stops retrying once it succeeds
func TestRetryAfterNetworkErrorRecovers(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		start := time.Now()
		err := <-exchange.RetryAfterNetworkError(t.Context(), "customex", "spot order execution limits", exchange.NetworkRetryDelays, func(context.Context) error {
			if calls.Add(1) == 1 {
				return errLookup
			}
			return nil
		})
		require.NoError(t, err, "the retry must report success")
		assert.Equal(t, int32(2), calls.Load(), "the step should be retried until it succeeds, then no more")
		assert.Equal(t, exchange.NetworkRetryDelays[0]+exchange.NetworkRetryDelays[1], time.Since(start), "each retry should wait its delay")
	})
}

// TestRetryAfterNetworkErrorGivesUp pins the bound: five attempts in all, from a second up to a minute
// apart, then the last network error is reported
func TestRetryAfterNetworkErrorGivesUp(t *testing.T) {
	t.Parallel()
	require.Equal(t, []time.Duration{time.Second, 4 * time.Second, 15 * time.Second, time.Minute}, exchange.NetworkRetryDelays, "the retries must wait from a second up to a minute")
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		start := time.Now()
		err := <-exchange.RetryAfterNetworkError(t.Context(), "customex", "credential validation", exchange.NetworkRetryDelays, func(context.Context) error {
			calls.Add(1)
			return errLookup
		})
		require.ErrorIs(t, err, errLookup, "the last network error must be reported")
		assert.Equal(t, int32(len(exchange.NetworkRetryDelays)), calls.Load(), "the step should be retried once per delay, five attempts with the first")
		assert.Equal(t, 80*time.Second, time.Since(start), "the retries should span the delays")
	})
}

func TestRetryAfterNetworkErrorStopsOnVenueAnswer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		err := <-exchange.RetryAfterNetworkError(t.Context(), "customex", "credential validation", exchange.NetworkRetryDelays, func(context.Context) error {
			calls.Add(1)
			return errVenueAnswer
		})
		require.ErrorIs(t, err, errVenueAnswer, "an answer from the venue must end the retries")
		assert.Equal(t, int32(1), calls.Load(), "an answer from the venue should not be retried")
	})
}

func TestRetryAfterNetworkErrorStopsWithContext(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var calls atomic.Int32
		done := exchange.RetryAfterNetworkError(ctx, "customex", "credential validation", exchange.NetworkRetryDelays, func(context.Context) error {
			calls.Add(1)
			return errLookup
		})
		time.Sleep(exchange.NetworkRetryDelays[0])
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-done, context.Canceled, "a done context must end the retries")
		assert.Equal(t, int32(1), calls.Load(), "no retry should run once the context is done")
	})
}

func TestRetryAfterNetworkErrorWithoutDelays(t *testing.T) {
	t.Parallel()
	called := false
	err := <-exchange.RetryAfterNetworkError(t.Context(), "customex", "credential validation", nil, func(context.Context) error {
		called = true
		return nil
	})
	require.Error(t, err, "no delays must be refused")
	assert.False(t, called, "nothing should be retried without delays")
}

type limitsEx struct {
	shared.CustomEx
	calls atomic.Int32
	fail  int32
}

func (l *limitsEx) GetAssetTypes(bool) asset.Items { return asset.Items{asset.Spot} }

func (l *limitsEx) UpdateOrderExecutionLimits(context.Context, asset.Item) error {
	if l.calls.Add(1) <= l.fail {
		return errLookup
	}
	return nil
}

// TestBootstrapRetriesLimitsAfterNetworkError pins AD-323 at start: limits that could not be loaded
// because the venue was unreachable are loaded again in the background, while the start still reports
// the failure as before
func TestBootstrapRetriesLimitsAfterNetworkError(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		l := &limitsEx{fail: 1}
		err := exchange.Bootstrap(t.Context(), l)
		require.ErrorIs(t, err, errLookup, "Bootstrap must still report the failed first load")
		assert.Equal(t, int32(1), l.calls.Load(), "Bootstrap should not wait for the retry")
		time.Sleep(exchange.NetworkRetryDelays[0])
		synctest.Wait()
		assert.Equal(t, int32(2), l.calls.Load(), "the limits should be loaded again after the first delay")
		time.Sleep(time.Hour)
		synctest.Wait()
		assert.Equal(t, int32(2), l.calls.Load(), "no retry should follow a successful load")
	})
}

type venueAnswerLimitsEx struct {
	limitsEx
}

func (v *venueAnswerLimitsEx) UpdateOrderExecutionLimits(context.Context, asset.Item) error {
	v.calls.Add(1)
	return errVenueAnswer
}

// TestBootstrapDoesNotRetryLimitsOnVenueAnswer pins that only a network failure is retried: a venue
// answer is reported and left alone
func TestBootstrapDoesNotRetryLimitsOnVenueAnswer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		v := &venueAnswerLimitsEx{}
		require.ErrorIs(t, exchange.Bootstrap(t.Context(), v), errVenueAnswer, "Bootstrap must report the venue's answer")
		time.Sleep(time.Hour)
		synctest.Wait()
		assert.Equal(t, int32(1), v.calls.Load(), "a venue answer should not be retried")
	})
}

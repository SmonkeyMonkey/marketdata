package exchange_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"marketdata/internal/exchange"
	"marketdata/internal/exchange/binance"
	"marketdata/internal/exchange/coinbase"
	"marketdata/internal/exchange/kraken"
)

// Trimmed real responses (2026-11-18).
const (
	coinbaseProducts = `[
	 {"id":"SOL-USD","base_currency":"SOL","quote_currency":"USD","status":"online","trading_disabled":false},
	 {"id":"OLD-USD","base_currency":"OLD","quote_currency":"USD","status":"delisted","trading_disabled":true},
	 {"id":"HALT-USD","base_currency":"HALT","quote_currency":"USD","status":"online","trading_disabled":true}]`
	binanceInfo = `{"symbols":[
	 {"symbol":"SOLUSDT","status":"TRADING","baseAsset":"SOL","quoteAsset":"USDT"},
	 {"symbol":"1000SATSUSDT","status":"TRADING","baseAsset":"1000SATS","quoteAsset":"USDT"},
	 {"symbol":"OLDBTC","status":"BREAK","baseAsset":"OLD","quoteAsset":"BTC"}]}`
	krakenPairs = `{"error":[],"result":{
	 "XXBTZUSD":{"wsname":"XBT/USD","status":"online"},
	 "XDGUSD":{"wsname":"XDG/USD","status":"online"},
	 "SOLUSD":{"wsname":"SOL/USD","status":"online"},
	 "FOOUSD":{"wsname":"FOO/USD","status":"cancel_only"}}}`
)

func serve(t *testing.T, body string) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
	t.Cleanup(s.Close)
	return s.URL
}

func symbols(t *testing.T, ins []exchange.Instrument, err error) []string {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, i := range ins {
		out = append(out, i.Symbol)
	}
	slices.Sort(out)
	return out
}

func TestCatalogs(t *testing.T) {
	ctx, c := context.Background(), http.DefaultClient
	for _, tc := range []struct {
		name string
		got  func() ([]exchange.Instrument, error)
		want []string
	}{
		{"coinbase", func() ([]exchange.Instrument, error) { return coinbase.Catalog(ctx, c, serve(t, coinbaseProducts)) }, []string{"SOL-USD"}},
		{"binance", func() ([]exchange.Instrument, error) { return binance.Catalog(ctx, c, serve(t, binanceInfo)) }, []string{"1000SATS-USDT", "SOL-USDT"}},
		{"kraken", func() ([]exchange.Instrument, error) { return kraken.Catalog(ctx, c, serve(t, krakenPairs)) }, []string{"BTC-USD", "DOGE-USD", "SOL-USD"}},
	} {
		ins, err := tc.got()
		if got := symbols(t, ins, err); !slices.Equal(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestCatalogHTTPError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTooManyRequests) }))
	defer s.Close()
	if _, err := binance.Catalog(context.Background(), http.DefaultClient, s.URL); err == nil {
		t.Fatal("want error on 429")
	}
}

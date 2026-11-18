package binance

import (
	"context"
	"net/http"

	"marketdata/internal/exchange"
)

const CatalogURL = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT&symbolStatus=TRADING"

// Catalog lists spot symbols that are trading.
func Catalog(ctx context.Context, c *http.Client, url string) ([]exchange.Instrument, error) {
	var info struct {
		Symbols []struct {
			Status string `json:"status"`
			Base   string `json:"baseAsset"`
			Quote  string `json:"quoteAsset"`
		} `json:"symbols"`
	}
	if err := exchange.FetchJSON(ctx, c, url, &info); err != nil {
		return nil, err
	}
	out := make([]exchange.Instrument, 0, len(info.Symbols))
	for _, s := range info.Symbols {
		if s.Status != "TRADING" {
			continue
		}
		if in, ok := exchange.NewInstrument(s.Base, s.Quote); ok {
			out = append(out, in)
		}
	}
	return out, nil
}

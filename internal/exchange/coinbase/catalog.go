package coinbase

import (
	"context"
	"net/http"

	"marketdata/internal/exchange"
)

const CatalogURL = "https://api.exchange.coinbase.com/products"

// Catalog lists products that are online and tradable.
func Catalog(ctx context.Context, c *http.Client, url string) ([]exchange.Instrument, error) {
	var products []struct {
		ID              string `json:"id"`
		Base            string `json:"base_currency"`
		Quote           string `json:"quote_currency"`
		Status          string `json:"status"`
		TradingDisabled bool   `json:"trading_disabled"`
	}
	if err := exchange.FetchJSON(ctx, c, url, &products); err != nil {
		return nil, err
	}
	out := make([]exchange.Instrument, 0, len(products))
	for _, p := range products {
		if p.Status != "online" || p.TradingDisabled {
			continue
		}
		if in, ok := exchange.NewInstrument(p.Base, p.Quote); ok && in.Symbol == p.ID {
			out = append(out, in)
		}
	}
	return out, nil
}

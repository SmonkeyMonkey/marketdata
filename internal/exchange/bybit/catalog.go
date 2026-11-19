package bybit

import (
	"context"
	"fmt"
	"net/http"

	"marketdata/internal/exchange"
)

// CatalogURL lists spot instruments; spot is returned in one page.
const CatalogURL = "https://api.bybit.com/v5/market/instruments-info?category=spot"

// Catalog lists spot symbols that are trading.
func Catalog(ctx context.Context, c *http.Client, url string) ([]exchange.Instrument, error) {
	var resp struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
		Result  struct {
			List []struct {
				Status string `json:"status"`
				Base   string `json:"baseCoin"`
				Quote  string `json:"quoteCoin"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := exchange.FetchJSON(ctx, c, url, &resp); err != nil {
		return nil, err
	}
	if resp.RetCode != 0 {
		return nil, fmt.Errorf("bybit: %s", resp.RetMsg)
	}
	out := make([]exchange.Instrument, 0, len(resp.Result.List))
	for _, s := range resp.Result.List {
		if s.Status != "Trading" {
			continue
		}
		if in, ok := exchange.NewInstrument(s.Base, s.Quote); ok {
			out = append(out, in)
		}
	}
	return out, nil
}

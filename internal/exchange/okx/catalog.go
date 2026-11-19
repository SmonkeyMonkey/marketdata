package okx

import (
	"context"
	"fmt"
	"net/http"

	"marketdata/internal/exchange"
)

const CatalogURL = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"

// Catalog lists spot instruments that are live (not pre-open or suspended).
func Catalog(ctx context.Context, c *http.Client, url string) ([]exchange.Instrument, error) {
	var resp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			State string `json:"state"`
			Base  string `json:"baseCcy"`
			Quote string `json:"quoteCcy"`
		} `json:"data"`
	}
	if err := exchange.FetchJSON(ctx, c, url, &resp); err != nil {
		return nil, err
	}
	if resp.Code != "0" {
		return nil, fmt.Errorf("okx: %s", resp.Msg)
	}
	out := make([]exchange.Instrument, 0, len(resp.Data))
	for _, s := range resp.Data {
		if s.State != "live" {
			continue
		}
		if in, ok := exchange.NewInstrument(s.Base, s.Quote); ok {
			out = append(out, in)
		}
	}
	return out, nil
}

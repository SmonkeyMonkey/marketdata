package kraken

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"marketdata/internal/exchange"
)

const CatalogURL = "https://api.kraken.com/0/public/AssetPairs"

// The REST API still uses Kraken's legacy asset codes in wsname, while the
// v2 websocket expects the common ones (checked: "XBT/USD" is rejected,
// "BTC/USD" accepted).
var v2Names = map[string]string{"XBT": "BTC", "XDG": "DOGE"}

// Catalog lists pairs that are online, named as the v2 websocket expects.
func Catalog(ctx context.Context, c *http.Client, url string) ([]exchange.Instrument, error) {
	var resp struct {
		Error  []string `json:"error"`
		Result map[string]struct {
			WSName string `json:"wsname"`
			Status string `json:"status"`
		} `json:"result"`
	}
	if err := exchange.FetchJSON(ctx, c, url, &resp); err != nil {
		return nil, err
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("kraken: %s", strings.Join(resp.Error, "; "))
	}
	out := make([]exchange.Instrument, 0, len(resp.Result))
	for _, p := range resp.Result {
		base, quote, ok := strings.Cut(p.WSName, "/")
		if p.Status != "online" || !ok {
			continue
		}
		if v, ok := v2Names[base]; ok {
			base = v
		}
		if v, ok := v2Names[quote]; ok {
			quote = v
		}
		if in, ok := exchange.NewInstrument(base, quote); ok {
			out = append(out, in)
		}
	}
	return out, nil
}

package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// FetchJSON GETs url and decodes the JSON body into dst (bodies are capped
// at 32 MiB; Binance's exchangeInfo is ~6 MiB).
func FetchJSON(ctx context.Context, c *http.Client, url string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "marketdata/1.0")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(dst)
}

// NewInstrument builds a canonical instrument, rejecting names our symbol
// format cannot represent.
func NewInstrument(base, quote string) (Instrument, bool) {
	sym := base + "-" + quote
	if _, _, err := SplitSymbol(sym); err != nil {
		return Instrument{}, false
	}
	return Instrument{Symbol: sym, Base: base, Quote: quote}, true
}

package exchange

import (
	"fmt"
	"strings"
)

// SplitSymbol splits a canonical BASE-QUOTE symbol (e.g. BTC-USDT).
func SplitSymbol(s string) (base, quote string, err error) {
	base, quote, ok := strings.Cut(s, "-")
	if !ok || base == "" || quote == "" || strings.Contains(quote, "-") || s != strings.ToUpper(s) {
		return "", "", fmt.Errorf("symbol %q is not canonical BASE-QUOTE in upper case", s)
	}
	return base, quote, nil
}

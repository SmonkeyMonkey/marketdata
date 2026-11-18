package store

import (
	"testing"

	"github.com/shopspring/decimal"
)

// Realistic 1m BTC volume: the ClickHouse-side division overflowed here.
func TestVWAPLargeVolumes(t *testing.T) {
	got := VWAP(decimal.RequireFromString("282794.123456789012345678"), decimal.RequireFromString("4.39919867"))
	if want := decimal.RequireFromString("64283.09896193"); !got.Equal(want) {
		t.Errorf("VWAP = %s, want %s", got, want)
	}
	if !VWAP(decimal.NewFromInt(5), decimal.Zero).IsZero() {
		t.Error("VWAP without volume must be 0")
	}
}

package codec

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

func TestTradeRoundTrip(t *testing.T) {
	in := model.Trade{
		Exchange:     "coinbase",
		Symbol:       "BTC-USD",
		TradeID:      "1133191511",
		Price:        decimal.RequireFromString("65007.40"),
		Size:         decimal.RequireFromString("0.00000002"),
		TakerSide:    model.SideSell,
		ExchangeTime: time.Date(2026, 11, 17, 14, 3, 46, 691275000, time.UTC),
		ReceiveTime:  time.Date(2026, 11, 17, 14, 3, 46, 741000123, time.UTC),
		Snapshot:     true,
	}
	b, err := EncodeTrade(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeTrade(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Exchange != in.Exchange || out.Symbol != in.Symbol || out.TradeID != in.TradeID ||
		!out.Price.Equal(in.Price) || !out.Size.Equal(in.Size) || out.TakerSide != in.TakerSide ||
		!out.ExchangeTime.Equal(in.ExchangeTime) || !out.ReceiveTime.Equal(in.ReceiveTime) || out.Snapshot != in.Snapshot {
		t.Errorf("round trip mismatch\n in: %+v\nout: %+v", in, out)
	}
}

func TestGapRoundTrip(t *testing.T) {
	in := model.Gap{Exchange: "coinbase", Symbol: "BTC-USD", AfterID: "10", BeforeID: "15", Missing: 4,
		DetectedAt: time.Date(2026, 11, 17, 0, 0, 0, 1, time.UTC), Source: "ingester-a"}
	b, err := EncodeGap(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeGap(b)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Errorf("round trip mismatch\n in: %+v\nout: %+v", in, out)
	}
}

func TestCandleRoundTrip(t *testing.T) {
	d := decimal.RequireFromString
	in := model.Candle{
		Exchange: "binance", Symbol: "BTC-USDT", Interval: time.Minute,
		Start: time.Date(2026, 11, 17, 15, 0, 0, 0, time.UTC),
		Open:  d("64000.1"), High: d("64100"), Low: d("63950.55"), Close: d("64010"),
		Volume: d("1.23456789"), QuoteVolume: d("79012.3456"), BuyVolume: d("0.5"), SellVolume: d("0.73456789"),
		Trades: 42, FirstTradeID: "100", LastTradeID: "141",
	}
	b, err := EncodeCandle(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeCandle(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Exchange != in.Exchange || out.Interval != in.Interval || !out.Start.Equal(in.Start) ||
		!out.Open.Equal(in.Open) || !out.Low.Equal(in.Low) || !out.QuoteVolume.Equal(in.QuoteVolume) ||
		!out.SellVolume.Equal(in.SellVolume) || out.Trades != in.Trades || out.LastTradeID != in.LastTradeID {
		t.Errorf("round trip mismatch\n in: %+v\nout: %+v", in, out)
	}
}

func TestZeroTimeSurvives(t *testing.T) {
	b, _ := EncodeTrade(model.Trade{Price: decimal.Zero, Size: decimal.Zero})
	out, err := DecodeTrade(b)
	if err != nil {
		t.Fatal(err)
	}
	if !out.ExchangeTime.IsZero() {
		t.Errorf("ExchangeTime = %v, want zero", out.ExchangeTime)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := DecodeTrade([]byte{0xff, 0xff}); err == nil {
		t.Fatal("want error")
	}
}

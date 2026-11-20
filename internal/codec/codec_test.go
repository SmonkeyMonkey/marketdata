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

func TestArbRoundTrip(t *testing.T) {
	at := time.Unix(1_800_000_000, 123_000_000).UTC()
	dec := decimal.RequireFromString
	snap := model.ArbSnapshot{Asset: "BTC-USD", Time: at,
		Venues: []model.ArbVenue{{Exchange: "binance", Symbol: "BTC-USDT", Bid: dec("64000.1"), Ask: dec("64000.2"),
			BidSize: dec("1.5"), AskSize: dec("0.2"), Rate: dec("0.9991"), RateSource: "kraken:USDT-USD", FeeBps: dec("10"), BookTime: at}},
		Routes: []model.ArbRoute{{BuyExchange: "kraken", BuySymbol: "BTC-USD", SellExchange: "binance", SellSymbol: "BTC-USDT",
			BuyPrice: dec("63990"), SellPrice: dec("64000.1"), GrossBps: dec("1.58"), NetBps: dec("-48.42"), Size: dec("0"), Profit: dec("0")}}}
	b, err := EncodeArbSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeArbSnapshot(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Asset != snap.Asset || !got.Time.Equal(at) || len(got.Venues) != 1 || len(got.Routes) != 1 ||
		!got.Venues[0].Rate.Equal(dec("0.9991")) || got.Venues[0].RateSource != "kraken:USDT-USD" || !got.Routes[0].NetBps.Equal(dec("-48.42")) {
		t.Errorf("snapshot round trip: %+v", got)
	}

	w := model.ArbWindow{ID: "w1", Asset: "BTC-USD", BuyExchange: "kraken", BuySymbol: "BTC-USD", SellExchange: "binance", SellSymbol: "BTC-USDT",
		Opened: at, Closed: at.Add(350 * time.Millisecond), PeakNetBps: dec("3.2"), PeakGrossBps: dec("53.2"),
		PeakSize: dec("0.4"), PeakProfit: dec("8.19"), Updates: 4}
	b, _ = EncodeArbWindow(w)
	gw, err := DecodeArbWindow(b)
	if err != nil {
		t.Fatal(err)
	}
	if gw.ID != w.ID || gw.Duration() != 350*time.Millisecond || !gw.PeakProfit.Equal(w.PeakProfit) || gw.Updates != 4 {
		t.Errorf("window round trip: %+v", gw)
	}
	if _, err := DecodeArbWindow([]byte{0x4a, 0x01, 'x'}); err == nil { // peak_net_bps = "x"
		t.Error("bad decimal accepted")
	}
}

var benchTrade = model.Trade{
	Exchange: "binance", Symbol: "BTC-USDT", TradeID: "6873309017",
	Price: decimal.RequireFromString("64626.55"), Size: decimal.RequireFromString("0.00008"),
	TakerSide: model.SideBuy, ExchangeTime: time.Unix(1_794_928_607, 766_000_000).UTC(), ReceiveTime: time.Unix(1_794_928_607, 812_000_000).UTC(),
}

func BenchmarkEncodeTrade(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := EncodeTrade(benchTrade); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeTrade(b *testing.B) {
	raw, _ := EncodeTrade(benchTrade)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeTrade(raw); err != nil {
			b.Fatal(err)
		}
	}
}

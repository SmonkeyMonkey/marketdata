package alerts

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

var t0 = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

func tr(id string, at time.Duration, price, size string) model.Trade {
	return model.Trade{Exchange: "binance", Symbol: "BTC-USDT", TradeID: id, TakerSide: model.SideBuy,
		Price: decimal.RequireFromString(price), Size: decimal.RequireFromString(size), ExchangeTime: t0.Add(at)}
}

func TestWhale(t *testing.T) {
	w := Whale{Threshold: decimal.NewFromInt(100_000)}
	if _, ok := w.Check(tr("1", 0, "65000", "1.5")); ok {
		t.Error("97.5k flagged")
	}
	a, ok := w.Check(tr("2", 0, "65000", "2"))
	if !ok || a.ID != "whale:binance:BTC-USDT:2" || a.Notional.String() != "130000" ||
		!strings.Contains(a.Message, "130000 USDT") {
		t.Errorf("alert = %+v", a)
	}
}

func move() *Move {
	return &Move{Pct: decimal.RequireFromString("1"), Window: time.Minute, Cooldown: 5 * time.Minute}
}

func TestMoveUpWithinWindow(t *testing.T) {
	m := move()
	m.Check(tr("1", 0, "100", "1"))
	if _, ok := m.Check(tr("2", 10*time.Second, "100.5", "1")); ok {
		t.Error("0.5% flagged")
	}
	a, ok := m.Check(tr("3", 30*time.Second, "101", "1"))
	if !ok || a.ChangePct.String() != "1" || !strings.Contains(a.Message, "up 1%") {
		t.Errorf("alert = %+v", a)
	}
}

func TestMoveIgnoresChangesOutsideWindow(t *testing.T) {
	m := move()
	m.Check(tr("1", 0, "100", "1"))
	if _, ok := m.Check(tr("2", 2*time.Minute, "105", "1")); ok {
		t.Error("move across more than the window flagged")
	}
}

func TestMoveDownAndCooldown(t *testing.T) {
	m := move()
	m.Check(tr("1", 0, "100", "1"))
	a, ok := m.Check(tr("2", 5*time.Second, "98", "1"))
	if !ok || a.ChangePct.String() != "-2" {
		t.Fatalf("down alert = %+v ok=%v", a, ok)
	}
	if _, ok := m.Check(tr("3", 10*time.Second, "97", "1")); ok {
		t.Error("fired again during cooldown")
	}
}

// The deques must track the true window min/max as old points expire.
func TestMoveWindowMinExpires(t *testing.T) {
	m := move()
	m.Cooldown = 0                 // otherwise the +11% alert below would mask the check
	m.Check(tr("1", 0, "90", "1")) // low, expires after 60s
	if _, ok := m.Check(tr("2", 30*time.Second, "100", "1")); !ok {
		t.Fatal("90 -> 100 within the window should fire")
	}
	if _, ok := m.Check(tr("3", 70*time.Second, "100.5", "1")); ok {
		t.Error("measured against an expired low") // vs 100 it is only +0.5%
	}
}

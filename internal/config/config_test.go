package config

import (
	"strings"
	"testing"
	"time"
)

func TestExchangeDefaults(t *testing.T) {
	c, err := LoadIngester()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, x := range c.Exchanges {
		names = append(names, x.Name)
	}
	if got := strings.Join(names, ","); got != "coinbase,binance,kraken,bybit,okx" {
		t.Errorf("exchanges = %s", got)
	}
}

func TestExchangeOverrides(t *testing.T) {
	t.Setenv("EXCHANGES", "kraken")
	t.Setenv("KRAKEN_SYMBOLS", "SOL-USD, BTC-EUR")
	t.Setenv("KRAKEN_READ_TIMEOUT", "5s")
	c, err := LoadIngester()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Exchanges) != 1 {
		t.Fatalf("got %d exchanges", len(c.Exchanges))
	}
	x := c.Exchanges[0]
	if strings.Join(x.Symbols, ",") != "SOL-USD,BTC-EUR" || x.ReadTimeout != 5*time.Second || x.URL != "wss://ws.kraken.com/v2" {
		t.Errorf("kraken = %+v", x)
	}
}

func TestAllErrorsReportedTogether(t *testing.T) {
	t.Setenv("EXCHANGES", "coinbase,ftx,coinbase")
	t.Setenv("COINBASE_READ_TIMEOUT", "soon")
	t.Setenv("LOG_FORMAT", "xml")
	_, err := LoadIngester()
	if err == nil {
		t.Fatal("want error")
	}
	for _, want := range []string{`unknown exchange "ftx"`, `"coinbase" listed twice`, "COINBASE_READ_TIMEOUT", "LOG_FORMAT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

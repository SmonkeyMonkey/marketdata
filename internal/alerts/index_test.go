package alerts

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

func rule(id string, dir model.Direction, price string) model.AlertRule {
	return model.AlertRule{ID: id, Exchange: "binance", Symbol: "BTC-USDT", Direction: dir, Price: decimal.RequireFromString(price)}
}

func fired(x *Index, price string) []string {
	var ids []string
	x.Match("binance", "BTC-USDT", decimal.RequireFromString(price), func(r model.AlertRule) { ids = append(ids, r.ID) })
	slices.Sort(ids)
	return ids
}

func TestFiresCrossedRulesOnce(t *testing.T) {
	x := NewIndex()
	for _, r := range []model.AlertRule{
		rule("a70k", model.Above, "70000"), rule("a71k", model.Above, "71000"), rule("a75k", model.Above, "75000"),
		rule("b60k", model.Below, "60000"), rule("b59k", model.Below, "59000"),
	} {
		if err := x.Put(r); err != nil {
			t.Fatal(err)
		}
	}
	if got := fired(x, "65000"); len(got) != 0 {
		t.Errorf("65000 fired %v", got)
	}
	if got := fired(x, "71000.00"); !slices.Equal(got, []string{"a70k", "a71k"}) {
		t.Errorf("71000 fired %v", got)
	}
	if got := fired(x, "72000"); len(got) != 0 {
		t.Errorf("rules fired twice: %v", got)
	}
	if got := fired(x, "59500"); !slices.Equal(got, []string{"b60k"}) {
		t.Errorf("59500 fired %v", got)
	}
	if x.Len() != 2 {
		t.Errorf("Len = %d, want 2 (a75k, b59k)", x.Len())
	}
}

func TestOtherSymbolsUntouched(t *testing.T) {
	x := NewIndex()
	_ = x.Put(rule("a", model.Above, "1"))
	var n int
	x.Match("binance", "ETH-USDT", decimal.NewFromInt(100), func(model.AlertRule) { n++ })
	x.Match("coinbase", "BTC-USDT", decimal.NewFromInt(100), func(model.AlertRule) { n++ })
	if n != 0 {
		t.Errorf("fired %d rules of another symbol", n)
	}
}

func TestRemoveAndReplace(t *testing.T) {
	x := NewIndex()
	_ = x.Put(rule("r", model.Above, "70000"))
	_ = x.Put(rule("r", model.Above, "80000")) // replace: the 70k version must be gone
	if got := fired(x, "75000"); len(got) != 0 {
		t.Errorf("replaced rule fired: %v", got)
	}
	if !x.Remove("r") || x.Remove("r") {
		t.Error("Remove should succeed once")
	}
	if got := fired(x, "90000"); len(got) != 0 {
		t.Errorf("removed rule fired: %v", got)
	}
}

// Random operations checked against a brute-force model.
func TestMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	x := NewIndex()
	ref := map[string]model.AlertRule{}
	for i := range 20_000 {
		switch op := rng.IntN(10); {
		case op < 6:
			dir := model.Above
			if rng.IntN(2) == 0 {
				dir = model.Below
			}
			r := rule(fmt.Sprint(rng.IntN(3000)), dir, fmt.Sprintf("%d.%02d", 60000+rng.IntN(10000), rng.IntN(100)))
			_ = x.Put(r)
			ref[r.ID] = r
		case op < 7:
			id := fmt.Sprint(rng.IntN(3000))
			if x.Remove(id) != (ref[id].ID != "") {
				t.Fatalf("op %d: Remove(%s) disagrees with reference", i, id)
			}
			delete(ref, id)
		default:
			p := decimal.NewFromInt(int64(60000 + rng.IntN(10000)))
			var want []string
			for id, r := range ref {
				if (r.Direction == model.Above && p.GreaterThanOrEqual(r.Price)) ||
					(r.Direction == model.Below && p.LessThanOrEqual(r.Price)) {
					want = append(want, id)
					delete(ref, id)
				}
			}
			slices.Sort(want)
			if got := fired(x, p.String()); !slices.Equal(got, want) {
				t.Fatalf("op %d: price %s fired %d rules, reference %d", i, p, len(got), len(want))
			}
		}
	}
	if x.Len() != len(ref) {
		t.Errorf("Len = %d, reference %d", x.Len(), len(ref))
	}
}

// --- benchmarks: one trade evaluated against n rules on its symbol.

func benchRules(n int) []model.AlertRule {
	rng := rand.New(rand.NewPCG(7, 8))
	rules := make([]model.AlertRule, n)
	for i := range rules {
		// Thresholds spread ±20% around 65k, so a random-walk price rarely crosses any.
		dir := model.Above
		px := 65000 + 1000 + rng.IntN(13000)
		if i%2 == 1 {
			dir, px = model.Below, 65000-1000-rng.IntN(13000)
		}
		rules[i] = rule(fmt.Sprint(i), dir, fmt.Sprint(px))
	}
	return rules
}

func BenchmarkIndexMatch(b *testing.B) {
	for _, n := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			x := NewIndex()
			for _, r := range benchRules(n) {
				_ = x.Put(r)
			}
			price := decimal.NewFromInt(65000)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				x.Match("binance", "BTC-USDT", price, func(model.AlertRule) {})
			}
		})
	}
}

// The naive alternative: scan every rule of the symbol on every trade.
func BenchmarkLinearScan(b *testing.B) {
	for _, n := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			rules := benchRules(n)
			px := make([]int64, n)
			for i, r := range rules {
				px[i] = toFixed(r.Price)
			}
			price := toFixed(decimal.NewFromInt(65000))
			b.ResetTimer()
			for range b.N {
				hits := 0
				for i, r := range rules {
					if (r.Direction == model.Above && price >= px[i]) || (r.Direction == model.Below && price <= px[i]) {
						hits++
					}
				}
				_ = hits
			}
		})
	}
}

package book

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

var d = decimal.RequireFromString

func prices(ls []model.BookLevel) []string {
	out := make([]string, len(ls))
	for i, l := range ls {
		out[i] = l.Price.String() + "x" + l.Size.String()
	}
	return out
}

func TestBestFirstUpdatesAndDeletes(t *testing.T) {
	b := New()
	b.SetBid(d("100"), d("1"))
	b.SetBid(d("101"), d("2"))
	b.SetBid(d("99.5"), d("3"))
	b.SetAsk(d("102"), d("1"))
	b.SetAsk(d("101.5"), d("4"))
	bids, asks := b.Top(10)
	if got := prices(bids); !slices.Equal(got, []string{"101x2", "100x1", "99.5x3"}) {
		t.Errorf("bids = %v", got)
	}
	if got := prices(asks); !slices.Equal(got, []string{"101.5x4", "102x1"}) {
		t.Errorf("asks = %v", got)
	}
	b.SetBid(d("100"), d("7"))    // update
	b.SetBid(d("101.00"), d("0")) // delete (same price, other notation)
	b.SetAsk(d("150"), d("0"))    // deleting a missing level is a no-op
	bids, _ = b.Top(1)
	if got := prices(bids); !slices.Equal(got, []string{"100x7"}) {
		t.Errorf("after update/delete = %v", got)
	}
}

func TestTruncateKeepsBest(t *testing.T) {
	b := New()
	for i := range 10 {
		b.SetBid(decimal.NewFromInt(int64(100+i)), d("1"))
		b.SetAsk(decimal.NewFromInt(int64(200+i)), d("1"))
	}
	b.Truncate(3)
	bids, asks := b.Top(10)
	if got := prices(bids); !slices.Equal(got, []string{"109x1", "108x1", "107x1"}) {
		t.Errorf("bids = %v", got)
	}
	if got := prices(asks); !slices.Equal(got, []string{"200x1", "201x1", "202x1"}) {
		t.Errorf("asks = %v", got)
	}
}

// Random updates against a plain map, compared after every operation.
func TestMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	b := New()
	ref := map[string]decimal.Decimal{}
	for i := range 20_000 {
		price := decimal.New(int64(60000_00+rng.IntN(2000)), -2)
		size := decimal.Zero
		if rng.IntN(3) > 0 {
			size = decimal.New(int64(rng.IntN(1000)), -3)
		}
		b.SetBid(price, size)
		if size.IsZero() {
			delete(ref, price.String())
		} else {
			ref[price.String()] = size
		}
		if i%500 == 0 {
			bids, _ := b.Top(len(ref) + 5)
			if len(bids) != len(ref) {
				t.Fatalf("op %d: %d levels, reference %d", i, len(bids), len(ref))
			}
			for j, l := range bids {
				if j > 0 && !l.Price.LessThan(bids[j-1].Price) {
					t.Fatalf("op %d: not sorted best first", i)
				}
				if want := ref[l.Price.String()]; !l.Size.Equal(want) {
					t.Fatalf("op %d: %s size %s, want %s", i, l.Price, l.Size, want)
				}
			}
		}
	}
}

func TestStats(t *testing.T) {
	bk := model.Book{
		Bids: []model.BookLevel{{Price: d("99"), Size: d("3")}, {Price: d("98"), Size: d("1")}},
		Asks: []model.BookLevel{{Price: d("101"), Size: d("1")}},
	}
	s, ok := bk.Stats(10)
	if !ok || !s.Mid.Equal(d("100")) || !s.Spread.Equal(d("2")) || !s.SpreadBps.Equal(d("200")) || !s.Imbalance.Equal(d("0.6")) {
		t.Errorf("stats = %+v", s)
	}
	if _, ok := (model.Book{}).Stats(10); ok {
		t.Error("empty book must not have stats")
	}
}

// Updates near the top of a 40k-level book (the common case on Coinbase).
func BenchmarkTopOfBookUpdate(b *testing.B) {
	bk := New()
	for i := range 40_000 {
		bk.SetBid(decimal.New(int64(6_000_000-i), -2), d("1"))
	}
	rng := rand.New(rand.NewPCG(1, 1))
	ups := make([]decimal.Decimal, 1024)
	for i := range ups {
		ups[i] = decimal.New(int64(6_000_000-rng.IntN(50)), -2)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		size := d("0")
		if i%2 == 0 {
			size = d("1.5")
		}
		bk.SetBid(ups[i%len(ups)], size)
	}
	_ = fmt.Sprint
}

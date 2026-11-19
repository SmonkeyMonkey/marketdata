package codec

import (
	"fmt"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"

	pb "marketdata/internal/gen/marketdata/v1"
	"marketdata/internal/model"
)

func EncodeArbSnapshot(s model.ArbSnapshot) ([]byte, error) {
	m := &pb.ArbSnapshot{Asset: s.Asset, TimeUnixNano: unixNano(s.Time)}
	for _, v := range s.Venues {
		m.Venues = append(m.Venues, &pb.ArbVenue{
			Exchange: v.Exchange, Symbol: v.Symbol,
			Bid: v.Bid.String(), Ask: v.Ask.String(), BidSize: v.BidSize.String(), AskSize: v.AskSize.String(),
			Rate: v.Rate.String(), RateSource: v.RateSource, TakerFeeBps: v.FeeBps.String(),
			BookTimeUnixNano: unixNano(v.BookTime),
		})
	}
	for _, r := range s.Routes {
		m.Routes = append(m.Routes, &pb.ArbRoute{
			BuyExchange: r.BuyExchange, BuySymbol: r.BuySymbol, SellExchange: r.SellExchange, SellSymbol: r.SellSymbol,
			BuyPrice: r.BuyPrice.String(), SellPrice: r.SellPrice.String(),
			GrossBps: r.GrossBps.String(), NetBps: r.NetBps.String(), Size: r.Size.String(), Profit: r.Profit.String(),
		})
	}
	return proto.Marshal(m)
}

func DecodeArbSnapshot(b []byte) (model.ArbSnapshot, error) {
	var m pb.ArbSnapshot
	if err := proto.Unmarshal(b, &m); err != nil {
		return model.ArbSnapshot{}, fmt.Errorf("decode arb snapshot: %w", err)
	}
	s := model.ArbSnapshot{Asset: m.Asset, Time: fromUnixNano(m.TimeUnixNano)}
	var d decimals
	for _, v := range m.Venues {
		s.Venues = append(s.Venues, model.ArbVenue{
			Exchange: v.Exchange, Symbol: v.Symbol,
			Bid: d.parse(v.Bid), Ask: d.parse(v.Ask), BidSize: d.parse(v.BidSize), AskSize: d.parse(v.AskSize),
			Rate: d.parse(v.Rate), RateSource: v.RateSource, FeeBps: d.parse(v.TakerFeeBps),
			BookTime: fromUnixNano(v.BookTimeUnixNano),
		})
	}
	for _, r := range m.Routes {
		s.Routes = append(s.Routes, model.ArbRoute{
			BuyExchange: r.BuyExchange, BuySymbol: r.BuySymbol, SellExchange: r.SellExchange, SellSymbol: r.SellSymbol,
			BuyPrice: d.parse(r.BuyPrice), SellPrice: d.parse(r.SellPrice),
			GrossBps: d.parse(r.GrossBps), NetBps: d.parse(r.NetBps), Size: d.parse(r.Size), Profit: d.parse(r.Profit),
		})
	}
	if d.err != nil {
		return model.ArbSnapshot{}, fmt.Errorf("decode arb snapshot: %w", d.err)
	}
	return s, nil
}

func EncodeArbWindow(w model.ArbWindow) ([]byte, error) {
	return proto.Marshal(&pb.ArbWindow{
		Id: w.ID, Asset: w.Asset,
		BuyExchange: w.BuyExchange, BuySymbol: w.BuySymbol, SellExchange: w.SellExchange, SellSymbol: w.SellSymbol,
		OpenedUnixNano: unixNano(w.Opened), ClosedUnixNano: unixNano(w.Closed),
		PeakNetBps: w.PeakNetBps.String(), PeakGrossBps: w.PeakGrossBps.String(),
		PeakSize: w.PeakSize.String(), PeakProfit: w.PeakProfit.String(), Updates: w.Updates,
	})
}

func DecodeArbWindow(b []byte) (model.ArbWindow, error) {
	var m pb.ArbWindow
	if err := proto.Unmarshal(b, &m); err != nil {
		return model.ArbWindow{}, fmt.Errorf("decode arb window: %w", err)
	}
	var d decimals
	w := model.ArbWindow{
		ID: m.Id, Asset: m.Asset,
		BuyExchange: m.BuyExchange, BuySymbol: m.BuySymbol, SellExchange: m.SellExchange, SellSymbol: m.SellSymbol,
		Opened: fromUnixNano(m.OpenedUnixNano), Closed: fromUnixNano(m.ClosedUnixNano),
		PeakNetBps: d.parse(m.PeakNetBps), PeakGrossBps: d.parse(m.PeakGrossBps),
		PeakSize: d.parse(m.PeakSize), PeakProfit: d.parse(m.PeakProfit), Updates: m.Updates,
	}
	if d.err != nil {
		return model.ArbWindow{}, fmt.Errorf("decode arb window: %w", d.err)
	}
	return w, nil
}

// decimals parses many fields and keeps the first error.
type decimals struct{ err error }

func (d *decimals) parse(s string) decimal.Decimal {
	v, err := decimal.NewFromString(s)
	if err != nil && d.err == nil {
		d.err = fmt.Errorf("decimal %q: %w", s, err)
	}
	return v
}

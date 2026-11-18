// Package settings holds per-market alert settings (whale thresholds) in a
// compacted Kafka topic. The alerts service reads them for every trade; the
// api service edits them from the UI.
package settings

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"

	"marketdata/internal/codec"
	"marketdata/internal/kvtable"
	"marketdata/internal/model"
)

// InitialWhale is the default whale threshold until one is saved from the
// UI. Thresholds are managed in the UI only, so this is not configurable.
var InitialWhale = decimal.NewFromInt(100_000)

type Store struct {
	*kvtable.Table[model.MarketSettings]
}

func Open(brokers []string, topic string, log *slog.Logger, opts ...kgo.Opt) (*Store, error) {
	t, err := kvtable.Open(brokers, topic, kvtable.Codec[model.MarketSettings]{
		Encode: codec.EncodeSettings,
		Decode: codec.DecodeSettings,
		Key:    model.MarketSettings.Key,
		Equal: func(a, b model.MarketSettings) bool {
			return a.WhaleNotional.Equal(b.WhaleNotional)
		},
	}, log, opts...)
	if err != nil {
		return nil, err
	}
	return &Store{Table: t}, nil
}

// DefaultWhale is the threshold of markets without their own setting.
func (s *Store) DefaultWhale() decimal.Decimal {
	if d, ok := s.Get(model.DefaultSettingsKey); ok {
		return d.WhaleNotional
	}
	return InitialWhale
}

// Whale is the threshold for one market. 0 disables whale alerts.
func (s *Store) Whale(exchange, symbol string) decimal.Decimal {
	if m, ok := s.Get(exchange + ":" + symbol); ok {
		return m.WhaleNotional
	}
	return s.DefaultWhale()
}

// Markets returns the per-market overrides, sorted.
func (s *Store) Markets() []model.MarketSettings {
	var out []model.MarketSettings
	for _, m := range s.All() {
		if m.Key() != model.DefaultSettingsKey {
			out = append(out, m)
		}
	}
	slices.SortFunc(out, func(a, b model.MarketSettings) int {
		return cmp.Or(cmp.Compare(a.Exchange, b.Exchange), cmp.Compare(a.Symbol, b.Symbol))
	})
	return out
}

func (s *Store) SetWhale(ctx context.Context, exchange, symbol string, v decimal.Decimal) error {
	return s.Put(ctx, model.MarketSettings{Exchange: exchange, Symbol: symbol, WhaleNotional: v, UpdatedAt: time.Now().UTC()})
}

// ResetWhale removes a market's own threshold so the default applies again.
func (s *Store) ResetWhale(ctx context.Context, exchange, symbol string) error {
	return s.Delete(ctx, exchange+":"+symbol)
}

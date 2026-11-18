// Package catalog caches the list of tradable instruments of each exchange.
package catalog

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"marketdata/internal/exchange"
)

type Fetcher func(ctx context.Context) ([]exchange.Instrument, error)

// Cache fetches an exchange's catalog on first use and refreshes it after
// TTL. One fetch per exchange runs at a time; if a refresh fails the last
// good list keeps being served.
type Cache struct {
	ttl      time.Duration
	fetchers map[string]Fetcher
	now      func() time.Time

	mu      sync.Mutex
	entries map[string]*entry
}

type entry struct {
	mu   sync.Mutex // serialises fetches of this exchange
	at   time.Time
	list []exchange.Instrument
	set  map[string]bool
}

func New(ttl time.Duration, fetchers map[string]Fetcher) *Cache {
	return &Cache{ttl: ttl, fetchers: fetchers, now: time.Now, entries: map[string]*entry{}}
}

func (c *Cache) Exchanges() []string {
	out := make([]string, 0, len(c.fetchers))
	for name := range c.fetchers {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

func (c *Cache) Get(ctx context.Context, exchangeName string) ([]exchange.Instrument, error) {
	e, err := c.load(ctx, exchangeName)
	if err != nil {
		return nil, err
	}
	return e.list, nil
}

// Has reports whether the exchange lists symbol as tradable.
func (c *Cache) Has(ctx context.Context, exchangeName, symbol string) (bool, error) {
	e, err := c.load(ctx, exchangeName)
	if err != nil {
		return false, err
	}
	return e.set[symbol], nil
}

func (c *Cache) load(ctx context.Context, name string) (*entry, error) {
	fetch, ok := c.fetchers[name]
	if !ok {
		return nil, fmt.Errorf("unknown exchange %q", name)
	}
	c.mu.Lock()
	e, ok := c.entries[name]
	if !ok {
		e = &entry{}
		c.entries[name] = e
	}
	c.mu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.list != nil && c.now().Sub(e.at) < c.ttl {
		return e, nil
	}
	list, err := fetch(ctx)
	if err != nil {
		if e.list != nil {
			return e, nil // stale beats nothing
		}
		return nil, fmt.Errorf("%s catalog: %w", name, err)
	}
	slices.SortFunc(list, func(a, b exchange.Instrument) int { return strings.Compare(a.Symbol, b.Symbol) })
	set := make(map[string]bool, len(list))
	for _, in := range list {
		set[in.Symbol] = true
	}
	e.list, e.set, e.at = list, set, c.now()
	return e, nil
}

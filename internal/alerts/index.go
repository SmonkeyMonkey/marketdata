// Package alerts evaluates user price rules and built-in detectors against
// the trade stream.
package alerts

import (
	"container/heap"
	"fmt"

	"github.com/shopspring/decimal"

	"marketdata/internal/model"
)

// priceScale turns decimal prices into int64 fixed point (8 decimals), so the
// hot path compares integers instead of allocating decimals.
const priceScale = 8

func toFixed(d decimal.Decimal) int64 { return d.Shift(priceScale).IntPart() }

// Index holds one-shot price rules.
//
// Per symbol, "above" rules sit in a min-heap by price and "below" rules in a
// max-heap. A trade at price p fires exactly the rules at the top of each heap
// that p has crossed, popping them; untouched rules are never looked at. A
// trade therefore costs O(1) when nothing fires and O(k log n) when k fire,
// whatever the number of rules n.
type Index struct {
	books map[string]*book
	byID  map[string]*entry
}

type book struct {
	above *ruleHeap // min price on top
	below *ruleHeap // max price on top
}

type entry struct {
	rule model.AlertRule
	px   int64
	pos  int // position in its heap, maintained by ruleHeap
	h    *ruleHeap
}

func NewIndex() *Index {
	return &Index{books: make(map[string]*book), byID: make(map[string]*entry)}
}

func key(exchange, symbol string) string { return exchange + ":" + symbol }

// Put adds a rule, replacing any rule with the same ID.
func (x *Index) Put(r model.AlertRule) error {
	if r.Direction != model.Above && r.Direction != model.Below {
		return fmt.Errorf("rule %s: invalid direction", r.ID)
	}
	x.Remove(r.ID)
	k := key(r.Exchange, r.Symbol)
	b, ok := x.books[k]
	if !ok {
		b = &book{
			above: &ruleHeap{less: func(a, b int64) bool { return a < b }},
			below: &ruleHeap{less: func(a, b int64) bool { return a > b }},
		}
		x.books[k] = b
	}
	e := &entry{rule: r, px: toFixed(r.Price)}
	e.h = b.above
	if r.Direction == model.Below {
		e.h = b.below
	}
	heap.Push(e.h, e)
	x.byID[r.ID] = e
	return nil
}

// Remove deletes a rule; it reports whether the rule existed.
func (x *Index) Remove(id string) bool {
	e, ok := x.byID[id]
	if !ok {
		return false
	}
	heap.Remove(e.h, e.pos)
	delete(x.byID, id)
	return true
}

// Match fires (and removes) every rule of exchange:symbol crossed by price.
func (x *Index) Match(exchange, symbol string, price decimal.Decimal, fire func(model.AlertRule)) {
	b, ok := x.books[key(exchange, symbol)]
	if !ok {
		return
	}
	px := toFixed(price)
	for b.above.Len() > 0 && b.above.items[0].px <= px {
		e := heap.Pop(b.above).(*entry)
		delete(x.byID, e.rule.ID)
		fire(e.rule)
	}
	for b.below.Len() > 0 && b.below.items[0].px >= px {
		e := heap.Pop(b.below).(*entry)
		delete(x.byID, e.rule.ID)
		fire(e.rule)
	}
}

func (x *Index) Len() int { return len(x.byID) }

func (x *Index) Get(id string) (model.AlertRule, bool) {
	e, ok := x.byID[id]
	if !ok {
		return model.AlertRule{}, false
	}
	return e.rule, true
}

// List returns the rules of one symbol, or all rules when exchange is empty.
func (x *Index) List(exchange, symbol string, limit int) []model.AlertRule {
	var out []model.AlertRule
	for _, e := range x.byID {
		if len(out) >= limit {
			break
		}
		if exchange == "" || (e.rule.Exchange == exchange && (symbol == "" || e.rule.Symbol == symbol)) {
			out = append(out, e.rule)
		}
	}
	return out
}

// ruleHeap implements heap.Interface and keeps entry.pos up to date so any
// rule can be removed in O(log n).
type ruleHeap struct {
	items []*entry
	less  func(a, b int64) bool
}

func (h *ruleHeap) Len() int           { return len(h.items) }
func (h *ruleHeap) Less(i, j int) bool { return h.less(h.items[i].px, h.items[j].px) }
func (h *ruleHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].pos, h.items[j].pos = i, j
}
func (h *ruleHeap) Push(v any) {
	e := v.(*entry)
	e.pos = len(h.items)
	h.items = append(h.items, e)
}
func (h *ruleHeap) Pop() any {
	n := len(h.items) - 1
	e := h.items[n]
	h.items[n] = nil
	h.items = h.items[:n]
	return e
}

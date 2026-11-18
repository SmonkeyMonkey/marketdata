// Package dedup remembers recently seen IDs per key with bounded memory.
package dedup

// Window keeps the last N IDs per key (e.g. per exchange:symbol). Redundant
// ingesters deliver the same trade within seconds of each other, so a short
// window catches nearly all duplicates; whatever slips through is collapsed
// later by the storage engine. Not safe for concurrent use.
type Window struct {
	size int
	keys map[string]*ring
}

type ring struct {
	ids  []string
	set  map[string]struct{}
	next int
}

func NewWindow(sizePerKey int) *Window {
	return &Window{size: sizePerKey, keys: make(map[string]*ring)}
}

// Seen reports whether id was recorded for key.
func (w *Window) Seen(key, id string) bool {
	r, ok := w.keys[key]
	if !ok {
		return false
	}
	_, ok = r.set[id]
	return ok
}

// Forget drops everything remembered for key.
func (w *Window) Forget(key string) { delete(w.keys, key) }

// Add records id for key, evicting the oldest ID once the window is full.
func (w *Window) Add(key, id string) {
	r, ok := w.keys[key]
	if !ok {
		r = &ring{ids: make([]string, 0, min(w.size, 1024)), set: make(map[string]struct{})}
		w.keys[key] = r
	}
	if _, dup := r.set[id]; dup {
		return
	}
	if len(r.ids) < w.size {
		r.ids = append(r.ids, id)
	} else {
		delete(r.set, r.ids[r.next])
		r.ids[r.next] = id
		r.next = (r.next + 1) % w.size
	}
	r.set[id] = struct{}{}
}

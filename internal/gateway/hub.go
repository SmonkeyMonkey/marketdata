// Package gateway streams market data to websocket clients.
package gateway

import (
	"slices"
	"sync"
)

// subscriber is what the hub needs from a connection.
type subscriber interface {
	// enqueue must not block; it returns false when the client's queue is full.
	enqueue(msg []byte) bool
	// slow is called (once per publish) for a subscriber whose queue was full.
	slow()
}

// Hub routes messages from channels to subscribers.
//
// Each channel's subscriber list is an immutable slice replaced on every
// change (copy-on-write): publishing takes the lock only to grab the current
// slice, then delivers without holding it, so broadcasts never wait on
// subscribe/unsubscribe and vice versa. Subscriptions change rarely compared
// to how often messages are published.
type Hub struct {
	mu       sync.Mutex
	channels map[string]*channelState
}

type channelState struct {
	subs []subscriber
	last []byte // latest message, sent to new subscribers as a snapshot
}

func NewHub() *Hub { return &Hub{channels: make(map[string]*channelState)} }

// Subscribe adds s to channel and returns the channel's latest message (nil
// if none yet), so the client gets current state without waiting.
func (h *Hub) Subscribe(channel string, s subscriber) (last []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.state(channel)
	if slices.Contains(st.subs, s) {
		return st.last
	}
	subs := make([]subscriber, len(st.subs), len(st.subs)+1)
	copy(subs, st.subs)
	st.subs = append(subs, s)
	return st.last
}

func (h *Hub) Unsubscribe(channel string, s subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st, ok := h.channels[channel]
	if !ok {
		return
	}
	i := slices.Index(st.subs, s)
	if i < 0 {
		return
	}
	subs := make([]subscriber, 0, len(st.subs)-1)
	subs = append(subs, st.subs[:i]...)
	st.subs = append(subs, st.subs[i+1:]...)
}

// Publish delivers msg to every subscriber of channel and returns how many
// received it. Subscribers whose queue is full are told they are slow.
func (h *Hub) Publish(channel string, msg []byte) (delivered int) {
	h.mu.Lock()
	st := h.state(channel)
	st.last = msg
	subs := st.subs
	h.mu.Unlock()

	for _, s := range subs {
		if s.enqueue(msg) {
			delivered++
		} else {
			s.slow()
		}
	}
	return delivered
}

// Subscribers returns the number of subscribers of channel.
func (h *Hub) Subscribers(channel string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if st, ok := h.channels[channel]; ok {
		return len(st.subs)
	}
	return 0
}

func (h *Hub) state(channel string) *channelState {
	st, ok := h.channels[channel]
	if !ok {
		st = &channelState{}
		h.channels[channel] = st
	}
	return st
}

// Package publish contains Publisher implementations.
package publish

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"marketdata/internal/model"
)

// JSONLines writes one JSON object per event: {"kind":"trade","data":{...}}.
// It is the stage-1 sink and stays useful for debugging (pipe into jq).
type JSONLines struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func NewJSONLines(w io.Writer) *JSONLines {
	return &JSONLines{enc: json.NewEncoder(w)}
}

type envelope struct {
	Kind string      `json:"kind"`
	Data model.Event `json:"data"`
}

func (p *JSONLines) Publish(_ context.Context, events []model.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range events {
		if err := p.enc.Encode(envelope{Kind: e.Kind(), Data: e}); err != nil {
			return err
		}
	}
	return nil
}

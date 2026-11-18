// Package backoff provides reconnect delay policies.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Policy returns how long to wait before retry number attempt (0-based).
type Policy interface {
	Delay(attempt int) time.Duration
}

// Exponential doubles the delay each attempt, capped at Max, with full jitter
// so many clients reconnecting after an exchange outage don't stampede.
type Exponential struct {
	Min, Max time.Duration
}

func (e Exponential) Delay(attempt int) time.Duration {
	d := e.Min
	for i := 0; i < attempt && d < e.Max; i++ {
		d *= 2
	}
	d = min(d, e.Max)
	if d <= e.Min {
		return e.Min
	}
	// Full jitter in [Min, d].
	return e.Min + rand.N(d-e.Min+1)
}

// Constant always waits the same duration.
type Constant time.Duration

func (c Constant) Delay(int) time.Duration { return time.Duration(c) }

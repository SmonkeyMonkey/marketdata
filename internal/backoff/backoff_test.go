package backoff

import (
	"testing"
	"time"
)

func TestExponentialStaysWithinBounds(t *testing.T) {
	p := Exponential{Min: 100 * time.Millisecond, Max: 2 * time.Second}
	for attempt := range 20 {
		ceil := p.Min << attempt
		if ceil > p.Max || ceil <= 0 {
			ceil = p.Max
		}
		for range 100 {
			d := p.Delay(attempt)
			if d < p.Min || d > ceil {
				t.Fatalf("attempt %d: delay %s outside [%s, %s]", attempt, d, p.Min, ceil)
			}
		}
	}
}

func TestExponentialFirstAttemptIsMin(t *testing.T) {
	p := Exponential{Min: time.Second, Max: time.Minute}
	if d := p.Delay(0); d != time.Second {
		t.Errorf("Delay(0) = %s, want 1s", d)
	}
}

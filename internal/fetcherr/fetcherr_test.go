package fetcherr

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fetches(errs ...error) kgo.Fetches {
	var parts []kgo.FetchPartition
	for i, err := range errs {
		parts = append(parts, kgo.FetchPartition{Partition: int32(i), Err: err})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "md.books", Partitions: parts}}}}
}

func watch() (*Watch, *time.Time, *bytes.Buffer) {
	var buf bytes.Buffer
	w := New(slog.New(slog.NewTextHandler(&buf, nil)), time.Minute)
	now := time.Unix(1000, 0)
	w.now = func() time.Time { return now }
	return w, &now, &buf
}

func TestPersistentErrorsFailAfterLimit(t *testing.T) {
	w, now, buf := watch()
	for i := 0; i < 600; i++ { // every 100ms for a minute
		if err := w.Check(fetches(kerr.UnknownTopicID, kerr.UnknownTopicID)); err != nil {
			t.Fatalf("failed early at %d: %v", i, err)
		}
		*now = now.Add(100 * time.Millisecond)
	}
	*now = now.Add(time.Millisecond) // just past the limit
	err := w.Check(fetches(kerr.UnknownTopicID))
	if !errors.Is(err, kerr.UnknownTopicID) {
		t.Fatalf("err = %v, want UNKNOWN_TOPIC_ID after the limit", err)
	}
	if logged := strings.Count(buf.String(), "kafka fetch error"); logged > 13 {
		t.Errorf("%d error lines for one minute, want at most one per 5s", logged)
	}
}

func TestShortOutagesDoNotAccumulate(t *testing.T) {
	w, now, buf := watch()
	for range 10 { // a 10s failure every 30s, for 5 minutes
		for range 10 {
			if err := w.Check(fetches(kerr.NotLeaderForPartition)); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
		}
		for range 20 {
			if err := w.Check(fetches()); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
		}
	}
	if !strings.Contains(buf.String(), "recovered") {
		t.Error("recovery not logged")
	}
}

func TestCancelledPollsAreNotErrors(t *testing.T) {
	w, now, _ := watch()
	for range 1000 {
		if err := w.Check(fetches(context.DeadlineExceeded, context.Canceled)); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(time.Second)
	}
}
